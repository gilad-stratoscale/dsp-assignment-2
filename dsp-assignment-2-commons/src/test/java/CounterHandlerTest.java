import dsp.Constants;
import dsp.CounterHandler;
import org.junit.Test;

/**
 * Created by thinkPAD on 6/12/2016.
 */
public class CounterHandlerTest {
    @Test
    public void testCounterWrite(){
        CounterHandler.writeCounter(Constants.Counters.WORD,123);
    }

}
