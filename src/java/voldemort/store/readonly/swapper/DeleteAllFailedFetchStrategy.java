package voldemort.store.readonly.swapper;

import java.util.Map;

/**
*
*/
class DeleteAllFailedFetchStrategy extends AbstractFailedFetchStrategy {
    private StoreSwapper storeSwapper;

    public DeleteAllFailedFetchStrategy(StoreSwapper storeSwapper) {
        super(storeSwapper);
        this.storeSwapper = storeSwapper;
    }

    @Override
    protected boolean dealWithIt(String storeName, long pushVersion, Map<Integer, String> results, Map<Integer, Exception> exceptions) {
        // Delete data from successful nodes
        for(int successfulNodeId: results.keySet()) {
            try {
                logger.info("Deleting fetched data from node " + successfulNodeId);

                storeSwapper.deleteOneNodeStoreVersion(successfulNodeId, storeName, results.get(successfulNodeId));
            } catch(Exception e) {
                logger.error("Exception thrown during delete operation on node "
                        + successfulNodeId + " : ", e);
            }
        }
        return false;
    }
}
