package voldemort.store.readonly;

import com.google.common.collect.Maps;
import voldemort.store.PersistenceFailureException;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Map;

/**
 * This class helps manage stores that have multiple versions of their data set.
 *
 * Currently, the class only supports the following functionality:
 * 1. Enabling/disabling specific store versions,
 * 2. Keeping track of which version is the current one.
 *
 * Note that 1 and 2 are orthogonal: the current version can be enabled or disabled.
 *
 * TODO: Port atomic swap functionality here.
 * TODO: Port delete backups functionality here.
 *
 * Eventually, this can allow us to generically manage stores with multiple data set
 * versions (not just ReadOnly).
 */
public class StoreVersionManager {
    private static final String DISABLED_MARKER_NAME = ".disabled";

    private final File rootDir;
    private final Map<Long, Boolean> versionToEnabledMap = Maps.newHashMap();
    private long currentVersion;

    /**
     * This constructor inspects the rootDir of the store and finds out which
     * versions exist and which one is active.
     *
     * @param rootDir of the store to be managed.
     */
    public StoreVersionManager(File rootDir) {
        this.rootDir = rootDir;
        // TODO: If the StoreVersionManager supports non-RO store in the future,
        // we should maybe move the functions below to another Utils class.
        File[] versionDirs = ReadOnlyUtils.getVersionDirs(rootDir);
        for (File versionDir: versionDirs) {
            long versionNumber = ReadOnlyUtils.getVersionId(versionDir);
            boolean versionEnabled = isVersionEnabled(versionDir);
            versionToEnabledMap.put(versionNumber, versionEnabled);
        }
        File currentVersionDir = ReadOnlyUtils.getCurrentVersion(rootDir);
        if (currentVersionDir != null) {
            currentVersion = ReadOnlyUtils.getVersionId(currentVersionDir);
        } else {
            currentVersion = -1; // Should we throw instead?
        }
    }

    /**
     * Enables a specific store/version, so that it stops failing requests.
     *
     * @param version to be enabled
     */
    public void enableStoreVersion(long version) throws PersistenceFailureException {
        versionToEnabledMap.put(version, true);
        persistEnabledVersion(version);
    }

    /**
     * Disables a specific store/version. When disabled, a store should
     * fail all subsequent requests to it.
     *
     * @param version to be disabled
     */
    public void disableStoreVersion(long version) throws PersistenceFailureException {
        versionToEnabledMap.put(version, false);
        persistDisabledVersion(version);
    }

    /**
     * @param version which
     * @return true if the requested version is enabled,
     *         false if the requested version is disabled,
     *         null if the requested version does not exist
     */
    public Boolean isStoreVersionEnabled(long version) {
        return versionToEnabledMap.get(version);
    }

    public Boolean isCurrentVersionEnabled() {
        return isStoreVersionEnabled(currentVersion);
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(long currentVersion) {
        this.currentVersion = currentVersion;
    }

    // PRIVATE UTILITY FUNCTIONS

    /**
     * Inspects the specified versionDir to see if it has been marked as disabled
     * (via a .disabled file in the directory). If the file is absent, the store is
     * assumed to be enabled.
     *
     * @param versionDir to inspect
     * @return true if the specified version is enabled, false otherwise
     * @throws IllegalArgumentException if the version does not exist
     */
    private boolean isVersionEnabled(File versionDir) throws IllegalArgumentException {
        File[] relevantFile = versionDir.listFiles(new FileFilter() {
            public boolean accept(File pathName) {
                return pathName.getName().equals(DISABLED_MARKER_NAME);
            }
        });
        return relevantFile.length == 1;
    }

    /**
     * Places a disabled marker file in the directory of the specified version.
     *
     * @param version to disable
     * @throws PersistenceFailureException if the marker file could not be created (can happen if
     *                                     the storage system has become read-only or is otherwise
     *                                     inaccessible).
     */
    private void persistDisabledVersion(long version) throws PersistenceFailureException {
        File versionDir = ReadOnlyUtils.getVersionDirs(rootDir, version, version)[0];
        File disabledMarker = new File(versionDir, DISABLED_MARKER_NAME);
        try {
            disabledMarker.createNewFile();
        } catch (IOException e) {
            throw new PersistenceFailureException(
                    "Failed to create the disabled marker in: " + versionDir.getAbsolutePath() +
                            "\nThe store/version will remain disabled only until the next restart.", e);
        }
    }

    /**
     * Deletes the disabled marker file in the directory of the specified version.
     *
     * @param version to enable
     * @throws PersistenceFailureException if the marker file could not be deleted (can happen if
     *                                     the storage system has become read-only or is otherwise
     *                                     inaccessible).
     */
    private void persistEnabledVersion(long version) throws PersistenceFailureException {
        File versionDir = ReadOnlyUtils.getVersionDirs(rootDir, version, version)[0];
        File disabledMarker = new File(versionDir, DISABLED_MARKER_NAME);
        if (disabledMarker.exists()) {
            if (!disabledMarker.delete()) {
                throw new PersistenceFailureException(
                        "Failed to delete the disabled marker in: " + versionDir.getAbsolutePath() +
                                "\nThe store/version will remain enabled only until the next restart.");
            }
        }
    }

}
