package voldemort.utils;

public class DynamicEventThrottler extends EventThrottler {

    private DynamicThrottleLimit dynThrottleLimit;

    public DynamicEventThrottler(DynamicThrottleLimit dynLimit) {
        super(dynLimit.getRate());
        this.dynThrottleLimit = Utils.notNull(dynLimit);
    }

    @Override
    public long getRate() {
        return dynThrottleLimit.getRate();
    }
}
