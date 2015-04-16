package voldemort.store.readonly.swapper;

import java.util.Map;

/**
*
*/
class DisableFailedOnlyFailedFetchStrategy extends AbstractFailedFetchStrategy {
    private static int MAX_NODE_FAILURES = 1;

    public DisableFailedOnlyFailedFetchStrategy(StoreSwapper storeSwapper) {
        super(storeSwapper);
    }

    @Override
    protected boolean dealWithIt(String storeName, long pushVersion, Map<Integer, String> results, Map<Integer, Exception> exceptions) throws Exception {
        if (exceptions.size() > MAX_NODE_FAILURES) {
            // Too many exceptions to tolerate this strategy... let's bail out.
            logger.error("We cannot use " + getClass().getSimpleName() +
                    " because there is more than " + MAX_NODE_FAILURES + " nodes that failed their fetches...");
            return false;
        } else {
            for (int nodeId: exceptions.keySet()) {
                storeSwapper.disableOneNodeStoreVersion(nodeId, storeName, pushVersion);
            }
            return true;
        }
    }
}
