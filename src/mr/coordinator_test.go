package mr

import (
	"testing"
)

func TestStartReduceStage(t *testing.T) {
	// assert reduce task queue has 3 tasks
	// Your code here.
	c := MakeCoordinator([]string{"a", "b"}, 3)
	c.startReduceStage()
	// assert stage is reduce
	if c.stage.Load() != reduceStage {
		t.Errorf("Expected stage to be reduce, got %v", c.stage.Load())
	}
	// assert reduce task queue has 3 tasks
	if c.idle_reduce_task_queue.Len() != 3 {
		t.Errorf("Expected reduce task queue to have 3 tasks, got %v", c.idle_reduce_task_queue.Len())
	}
	// assert reduce task status has 3 tasks and all are unstarted
	if len(c.reduce_task_status) != 3 {
		t.Errorf("Expected reduce task status to have 3 tasks, got %v", len(c.reduce_task_status))
	}
	for _, status := range c.reduce_task_status {
		if status.Status != unstarted {
			t.Errorf("Expected reduce task status to be unstarted, got %v", status.Status)
		}
	}
	
}