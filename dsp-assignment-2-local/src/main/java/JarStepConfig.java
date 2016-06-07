import java.util.Collection;
import java.util.List;

/**
 * Created by thinkPAD on 6/7/2016.
 */
public class JarStepConfig {
    String jarUrl;
    String cp;
    Collection<String> args;
    String stepName;

    public JarStepConfig(String jarUrl, String cp, Collection<String> args, String stepName) {
        this.jarUrl = jarUrl;
        this.cp = cp;
        this.args=args;
        this.stepName = stepName;
    }
}
