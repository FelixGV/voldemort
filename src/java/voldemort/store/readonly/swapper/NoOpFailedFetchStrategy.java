package voldemort.store.readonly.swapper;

import java.util.Map;

/**
*
*/
class NoOpFailedFetchStrategy extends AbstractFailedFetchStrategy {
    public NoOpFailedFetchStrategy(StoreSwapper storeSwapper) {
        super(storeSwapper);
    }

    @Override
    protected boolean dealWithIt(String storeName, long pushVersion, Map<Integer, String> results, Map<Integer, Exception> exceptions) {
        return false;
    }
}
