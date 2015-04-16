package voldemort.store.readonly.hooks;

/**
 * This enum describes the various stages a Build and Push can be in.
 */
public enum BuildAndPushStatus {
    // The "happy path" status:
    STARTING,               // emitted once per job
    BUILDING,               // emitted once per job
    PUSHING,                // emitted once per colo
    SWAPPED,                // emitted once per colo
    FINISHED(true),         // emitted once per job
    HEARTBEAT,              // emitted periodically during any of the stages...

    // The "not-so-happy path" status:
    SWAPPED_WITH_FAILURES,  // emitted for each colo that failed to push completely but still managed to swap
    CANCELLED(true),        // emitted once per job
    FAILED(true);           // emitted once per job

    public final boolean isTerminal;
    private BuildAndPushStatus() {
        this(false);
    }
    private BuildAndPushStatus(boolean terminationStatus) {
        isTerminal = terminationStatus;
    }
}