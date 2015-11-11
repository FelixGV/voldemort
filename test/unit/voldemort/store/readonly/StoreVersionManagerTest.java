package voldemort.store.readonly;

import junit.framework.Assert;
import org.junit.Test;
import junit.framework.TestCase;
import voldemort.TestUtils;

import java.io.File;

/**
 * Basic tests for {@link StoreVersionManager}
 */
public class StoreVersionManagerTest extends TestCase {
    @Test
    public void testDisableStoreVersion() {
        File rootDir = TestUtils.createTempDir();
        File version0 = new File(rootDir, "version-0");
        File version1 = new File(rootDir, "version-1");
        Assert.assertTrue("Failed to create version directory!", version0.mkdir());
        Assert.assertTrue("Failed to create version directory!", version1.mkdir());
        StoreVersionManager storeVersionManager = new StoreVersionManager(rootDir);
        storeVersionManager.syncInternalStateFromFileSystem();
        Assert.assertFalse("Did not expect to have any store version disabled.",
                           storeVersionManager.hasAnyDisabledVersion());
        storeVersionManager.disableStoreVersion(0);
        Assert.assertTrue("Expected to have some store version disabled.",
                           storeVersionManager.hasAnyDisabledVersion());
        Assert.assertTrue("Expected store version 1 to be enabled.",
                          storeVersionManager.isCurrentVersionEnabled());
    }
}
