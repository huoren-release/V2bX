package task

import (
	"log"
	"sync/atomic"
	"testing"
	"time"
)

func TestTask(t *testing.T) {
	ts := Task{Execute: func() error {
		log.Println("q")
		return nil
	}, Interval: time.Second}
	ts.Start(false)
	ts.Close()
}

// Close() 之后重新 Start()，旧实现会让上一代循环继续跑（它在 select 里读的是
// 被换掉的 t.stop，永远看不到关闭信号），于是每轮重启都多一个不死的循环。
// 用执行次数判断：只有一个循环存活时，50ms / 20ms 间隔最多跑 3 次。
func TestRestartDoesNotDuplicateLoop(t *testing.T) {
	var n int64
	tk := &Task{
		Interval: 20 * time.Millisecond,
		Execute:  func() error { atomic.AddInt64(&n, 1); return nil },
	}

	if err := tk.Start(false); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(70 * time.Millisecond)
	tk.Close()
	time.Sleep(30 * time.Millisecond)

	if err := tk.Start(false); err != nil {
		t.Fatalf("restart: %v", err)
	}
	start := atomic.LoadInt64(&n)
	time.Sleep(50 * time.Millisecond)
	delta := atomic.LoadInt64(&n) - start
	tk.Close()
	time.Sleep(50 * time.Millisecond)

	if delta > 3 {
		t.Fatalf("restart left an extra loop running: %d executions in 50ms (want <=3)", delta)
	}
}

// 反复 Close/Start 不应该出现 panic（二次 close）或数量失控。
func TestRepeatedRestart(t *testing.T) {
	var n int64
	tk := &Task{
		Interval: 10 * time.Millisecond,
		Execute:  func() error { atomic.AddInt64(&n, 1); return nil },
	}
	for i := 0; i < 20; i++ {
		if err := tk.Start(false); err != nil {
			t.Fatalf("start #%d: %v", i, err)
		}
		time.Sleep(5 * time.Millisecond)
		tk.Close()
	}
	start := atomic.LoadInt64(&n)
	time.Sleep(60 * time.Millisecond)
	delta := atomic.LoadInt64(&n) - start
	if delta > 3 {
		t.Fatalf("closed task kept executing: %d executions after final Close", delta)
	}
}

// Execute 阻塞期间 Close+Start：老实现里老循环会在 Execute 返回后继续活着。
func TestCloseWhileExecuteBlocked(t *testing.T) {
	block := make(chan struct{})
	var n int64
	tk := &Task{
		Interval: 10 * time.Millisecond,
		Execute: func() error {
			if atomic.AddInt64(&n, 1) == 1 {
				<-block // 第一次执行卡住，模拟上报 HTTP 调用
			}
			return nil
		},
	}
	if err := tk.Start(false); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	tk.Close()
	if err := tk.Start(false); err != nil {
		t.Fatalf("restart: %v", err)
	}
	close(block) // 放行被卡住的那一代
	time.Sleep(60 * time.Millisecond)

	start := atomic.LoadInt64(&n)
	time.Sleep(50 * time.Millisecond)
	delta := atomic.LoadInt64(&n) - start
	tk.Close()
	if delta > 8 {
		t.Fatalf("blocked generation kept looping after Close: %d executions", delta)
	}
}