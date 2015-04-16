package voldemort.store.readonly.swapper;

import org.apache.log4j.Logger;

import java.util.Map;

/**
*
*/
abstract class AbstractFailedFetchStrategy {
    protected final Logger logger = Logger.getLogger(this.getClass().getName());

    protected final StoreSwapper storeSwapper;

    public AbstractFailedFetchStrategy(StoreSwapper storeSwapper) {
        this.storeSwapper = storeSwapper;
    }

    /**
     *
     * @param storeName name of the store affected by the failed fetch
     * @param pushVersion version of the store affected by the failed fetch
     * @param results map of <node ID, results> for successful operations.
     * @param exceptions map of <node ID, exceptions> for failed operations.
     * @return true if the store/version is in a condition where the swap can be done, false otherwise.
     * @throws Exception
     */
    protected abstract boolean dealWithIt(String storeName,
                                          long pushVersion,
                                          Map<Integer, String> results,
                                          Map<Integer, Exception> exceptions) throws Exception;
}
