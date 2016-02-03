package storm.playground;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;

@Test
public class ReplayingSpoutTest extends Assert {

    @Test
    public void shouldResendEqualTupleIfFailed() throws Exception {
        TestSpoutOutputCollector spy = spy(new TestSpoutOutputCollector());
        TopologyContext context = mock(TopologyContext.class, RETURNS_DEEP_STUBS);

        ReplayingSpout spout = new ReplayingSpout(1);
        spout.open(new HashMap<>(), context, new SpoutOutputCollector(spy));
        spout.nextTuple();
        spout.fail(spy.messageId);
        verify(spy, times(2)).emit(anyString(), eq(spy.tuple), eq(spy.messageId));
    }

    static class TestSpoutOutputCollector implements ISpoutOutputCollector {
        Object messageId;
        List<Object> tuple;

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            this.messageId = messageId;
            this.tuple = tuple;
            return new ArrayList<>();
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        }

        @Override
        public void reportError(Throwable error) {
        }
    }
}
