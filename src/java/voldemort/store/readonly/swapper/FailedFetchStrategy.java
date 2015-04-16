package voldemort.store.readonly.swapper;

import com.sleepycat.je.rep.impl.RepGroupProtocol;

/**
 *
 */
public enum FailedFetchStrategy {
    NO_OP,
    DELETE_ALL,
    DISABLE_FAILED_ONLY;

    public static AbstractFailedFetchStrategy getStrategyInstance(StoreSwapper storeSwapper,
                                                           FailedFetchStrategy failedFetchStrategy) {
        switch (failedFetchStrategy) {
            case NO_OP: return new NoOpFailedFetchStrategy(storeSwapper);
            case DELETE_ALL: return new DeleteAllFailedFetchStrategy(storeSwapper);
            case DISABLE_FAILED_ONLY: return new DisableFailedOnlyFailedFetchStrategy(storeSwapper);
            default: throw new IllegalArgumentException("The specified FailedFetchStrategy is not supported!");
        }
    }
}
