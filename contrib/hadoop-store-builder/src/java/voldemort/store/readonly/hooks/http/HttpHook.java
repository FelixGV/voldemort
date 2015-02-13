package voldemort.store.readonly.hooks.http;

import com.google.common.collect.Lists;
import voldemort.store.readonly.hooks.AbstractBuildAndPushHook;
import voldemort.store.readonly.hooks.BuildAndPushStatus;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class HttpHook extends AbstractBuildAndPushHook {

    // Config keys
    private final String URL_TO_CALL = configKeyPrefix + "url";
    private final String EXECUTOR_THREADS = configKeyPrefix + "num-threads";

    // Config values
    private String urlToCall = null;

    // Execution
    private ExecutorService executorService;
    private final List<Future> httpFutureResults = Lists.newArrayList();
    private final List<BuildAndPushStatus> terminationStatuses = Lists.newArrayList(
            BuildAndPushStatus.FINISHED,
            BuildAndPushStatus.CANCELLED,
            BuildAndPushStatus.FAILED);
    private final List<BuildAndPushStatus> statusesToCallHookFor = getStatusListToCallHookFor();

    @Override
    public void init(Properties properties) throws Exception {
        this.urlToCall = getStringPropertyOrFail(properties, URL_TO_CALL);
        int numThreads = getIntProperty(properties, EXECUTOR_THREADS, "1");
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }

    @Override
    public void invoke(BuildAndPushStatus buildAndPushStatus, String s) {
        if (statusesToCallHookFor.contains(buildAndPushStatus)) {
            httpFutureResults.add(this.executorService.submit(new HttpHookRunnable(
                    getName(),
                    log,
                    urlToCall,
                    getHttpMethod(buildAndPushStatus),
                    getContentType(buildAndPushStatus),
                    getRequestBody(buildAndPushStatus))));
        }
        if (terminationStatuses.contains(buildAndPushStatus)) {
            cleanUp();
        }
    }

    /**
     * Override this function if you need another http method than POST.
     *
     * @param buildAndPushStatus
     * @return the method to use in the HTTP request
     */
    protected HttpMethod getHttpMethod(BuildAndPushStatus buildAndPushStatus) {
        return HttpMethod.POST;
    }

    /**
     * Override this function if you need a Content-type header specified.
     *
     * The default implementation (returning null) will cause no Content-type header
     * at all to be used.
     *
     * @param buildAndPushStatus
     * @return the content-type to use in the HTTP request, can be null
     */
    protected String getContentType(BuildAndPushStatus buildAndPushStatus) {
        return null;
    }

    /**
     * Concrete classes must implement this to declare which {@link BuildAndPushStatus}
     * the hook should send an HTTP request for.
     *
     * @return a list of {@link BuildAndPushStatus} to act on
     */
    protected abstract List<BuildAndPushStatus> getStatusListToCallHookFor();

    /**
     * Concrete classes must implement this to provide a request body to include in the
     * HTTP request. Can be empty string, but cannot be null.
     *
     * @param buildAndPushStatus
     * @return the request body to include in the HTTP request
     */
    protected abstract String getRequestBody(BuildAndPushStatus buildAndPushStatus);

    private void cleanUp() {
        for (Future result : httpFutureResults) {
            try {
                result.get();
            } catch (Exception e) {
                this.log.error("Exception while getting the result of the " +
                        getName() + "'s HTTP request...", e);
            }
        }
        this.executorService.shutdownNow();
    }
}
