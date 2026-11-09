package lab

import (
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"k8s.io/client-go/util/workqueue"
)

// QueueProbe lets crashtest see inside a client-go style controller: one with
// a workqueue field and a sync function field, like kthena's controllers and
// Kubernetes' sample-controller. It tells the runner when the controller is
// idle, counts reconciles and errors, and can queue everything again.
type QueueProbe struct {
	name string
	q    *probeQueue

	mu         sync.Mutex
	reconciles int
	errors     int
	lastError  string
}

// WatchQueue replaces the controller's work queue (queueField) and wraps its
// sync function (syncField, optional) so crashtest can watch them. Both are
// usually unexported, so it sets them through reflection. Call it after the
// controller is built and before it runs. If kthena renames a field, this
// returns an error naming it.
func WatchQueue(name string, ctrl any, queueField, syncField string) (*QueueProbe, error) {
	v := reflect.ValueOf(ctrl)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("crashtest: WatchQueue needs a pointer to a controller struct, got %T", ctrl)
	}
	limiter := workqueue.DefaultTypedControllerRateLimiter[any]()
	p := &QueueProbe{name: name, q: &probeQueue{
		TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueueWithConfig(limiter, workqueue.TypedRateLimitingQueueConfig[any]{}),
		limiter:                    limiter,
		keys:                       map[any]bool{},
	}}

	qf, err := field(v, queueField)
	if err != nil {
		return nil, err
	}
	if !reflect.TypeOf(p.q).AssignableTo(qf.Type()) {
		return nil, fmt.Errorf("crashtest: %T.%s is a %s; crashtest's queue does not fit it", ctrl, queueField, qf.Type())
	}
	if old, ok := qf.Interface().(interface{ ShutDown() }); ok && !qf.IsNil() {
		old.ShutDown()
	}
	qf.Set(reflect.ValueOf(p.q))

	if syncField == "" {
		return p, nil
	}
	sf, err := field(v, syncField)
	if err != nil {
		return nil, err
	}
	if sf.Kind() != reflect.Func || sf.IsNil() {
		return nil, fmt.Errorf("crashtest: %T.%s is not a function that is set", ctrl, syncField)
	}
	sync := reflect.ValueOf(sf.Interface())
	sf.Set(reflect.MakeFunc(sf.Type(), func(args []reflect.Value) []reflect.Value {
		out := sync.Call(args)
		p.count(out)
		return out
	}))
	return p, nil
}

// field returns a settable view of a (possibly unexported) struct field.
func field(v reflect.Value, name string) (reflect.Value, error) {
	f := v.Elem().FieldByName(name)
	if !f.IsValid() {
		return reflect.Value{}, fmt.Errorf("crashtest: %s has no field %q; the controller changed, update its target", v.Type(), name)
	}
	return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem(), nil
}

func (p *QueueProbe) count(out []reflect.Value) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reconciles++
	if n := len(out); n > 0 {
		if err, ok := out[n-1].Interface().(error); ok && err != nil {
			p.errors++
			p.lastError = err.Error()
		}
	}
}

// Busy implements Controller.
func (p *QueueProbe) Busy(horizon time.Duration) (bool, string) { return p.q.busy(horizon) }

// Resync implements Controller.
func (p *QueueProbe) Resync() { p.q.resync() }

// Drained reports whether no reconcile is running. After a process is
// stopped, the runner waits for this before starting the next one: a real
// crashed process runs nothing, and kthena's constructor writes to
// client-go's global scheme, which a still-running reconcile reads.
func (p *QueueProbe) Drained() bool {
	p.q.mu.Lock()
	defer p.q.mu.Unlock()
	return p.q.inFlight == 0
}

// Stats implements Controller.
func (p *QueueProbe) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{Name: p.name, Reconciles: p.reconciles, Errors: p.errors, LastError: p.lastError}
}

// probeQueue is a real client-go rate-limited queue that also keeps track of
// what is in flight and which requeues are scheduled.
type probeQueue struct {
	workqueue.TypedRateLimitingInterface[any]
	limiter workqueue.TypedRateLimiter[any]

	mu       sync.Mutex
	inFlight int
	timers   []time.Time // when delayed adds (AddAfter, AddRateLimited) fire
	keys     map[any]bool
}

// A requeue is treated as pending for this long after it is due, so the
// moment between the timer firing and the item showing up in the queue
// does not look idle.
const timerGrace = 50 * time.Millisecond

func (q *probeQueue) Add(item any) {
	q.mu.Lock()
	q.keys[item] = true
	q.mu.Unlock()
	q.TypedRateLimitingInterface.Add(item)
}

func (q *probeQueue) AddAfter(item any, d time.Duration) {
	q.mu.Lock()
	q.keys[item] = true
	if d > 0 {
		q.timers = append(q.timers, time.Now().Add(d))
	}
	q.mu.Unlock()
	q.TypedRateLimitingInterface.AddAfter(item, d)
}

func (q *probeQueue) AddRateLimited(item any) { q.AddAfter(item, q.limiter.When(item)) }

func (q *probeQueue) Get() (any, bool) {
	item, shutdown := q.TypedRateLimitingInterface.Get()
	if !shutdown {
		q.mu.Lock()
		q.inFlight++
		q.mu.Unlock()
	}
	return item, shutdown
}

func (q *probeQueue) Done(item any) {
	q.TypedRateLimitingInterface.Done(item)
	q.mu.Lock()
	q.inFlight--
	q.mu.Unlock()
}

func (q *probeQueue) busy(horizon time.Duration) (bool, string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	live := q.timers[:0]
	for _, t := range q.timers {
		if now.Sub(t) < timerGrace {
			live = append(live, t)
		}
	}
	q.timers = live
	if q.inFlight > 0 {
		return true, "reconciling"
	}
	if n := q.TypedRateLimitingInterface.Len(); n > 0 {
		return true, fmt.Sprintf("%d keys queued", n)
	}
	for _, t := range q.timers {
		if d := t.Sub(now); d <= horizon {
			return true, fmt.Sprintf("requeue due in %s", d.Round(time.Millisecond))
		}
	}
	return false, ""
}

func (q *probeQueue) resync() {
	q.mu.Lock()
	keys := make([]any, 0, len(q.keys))
	for k := range q.keys {
		keys = append(keys, k)
	}
	q.mu.Unlock()
	for _, k := range keys {
		q.TypedRateLimitingInterface.Add(k)
	}
}
