package voldemort.store.readonly.swapper;

import voldemort.utils.Props;

import java.util.Set;

/**
 * Component to make sure we can do some operations synchronously across many processes.
 */
public abstract class FailedFetchLock {
    protected final Props props;
    protected final String clusterId;
    public FailedFetchLock(Props props, String clusterId) {
        this.props = props;
        this.clusterId = clusterId;
    }
    public abstract void acquireLock() throws Exception;
    public abstract void releaseLock() throws Exception;
    public abstract Set<Integer> getDisabledNodes() throws Exception;
    public abstract void addDisabledNode(int nodeId,
                                         String details,
                                         String storeName,
                                         long storeVersion) throws Exception;
}