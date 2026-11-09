package task

import (
	"sync"
	"time"
)

type Task struct {
	Interval time.Duration
	Execute  func() error
	access   sync.Mutex
	running  bool
	stop     chan struct{}
}

func (t *Task) Start(first bool) error {
	t.access.Lock()
	if t.running {
		t.access.Unlock()
		return nil
	}
	t.running = true
	// 每一代都持有自己的退出通道：Close() 之后重启时，上一代 goroutine 即便
	// 正卡在 Execute() 里，也会在下一轮 select 看到自己那个已关闭的通道而退出。
	// 旧实现把 t.stop 直接放在 select 里读，Start() 换掉通道后老 goroutine 就
	// 永远看不到关闭信号，于是每轮 Close/Start 都多留一个不会退出的循环。
	stop := make(chan struct{})
	t.stop = stop
	t.access.Unlock()

	go func() {
		// 收尾：只有本代仍是当前代、且仍标记 running 才改状态并关通道，
		// 避免 Close() 关过之后这里二次 close 触发 panic。
		finish := func() {
			t.access.Lock()
			if t.running && t.stop == stop {
				t.running = false
				close(stop)
			}
			t.access.Unlock()
		}

		if first {
			if err := t.Execute(); err != nil {
				finish()
				return
			}
		}

		for {
			select {
			case <-time.After(t.Interval):
			case <-stop:
				return
			}

			if err := t.Execute(); err != nil {
				finish()
				return
			}
		}
	}()

	return nil
}

func (t *Task) Close() {
	t.access.Lock()
	if t.running {
		t.running = false
		close(t.stop)
	}
	t.access.Unlock()
}
