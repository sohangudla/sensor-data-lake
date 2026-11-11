package dev.sensorlake.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * LOCAL DEVELOPMENT ON WINDOWS ONLY.
 *
 * Hadoop's local file system applies POSIX permissions (chmod/chown, "ls -l")
 * by shelling out to winutils.exe on Windows. That binary is not shipped by
 * Apache, and Windows has no POSIX permissions anyway. This subclass keeps
 * every read/write/rename/delete operation from RawLocalFileSystem but turns
 * the permission calls into no-ops and builds FileStatus objects from
 * java.io.File directly.
 *
 * Enabled only when the pipeline runs on Windows, via
 *   spark.hadoop.fs.file.impl = dev.sensorlake.hadoop.WindowsLocalFileSystem
 * On Linux / AWS Glue the standard Hadoop file systems are used.
 */
public class WindowsLocalFileSystem extends RawLocalFileSystem {

    private static final FsPermission DEFAULT_PERMISSION = FsPermission.getDefault();

    @Override
    public void setPermission(Path p, FsPermission permission) {
        // no-op: Windows has no POSIX permissions
    }

    @Override
    public void setOwner(Path p, String username, String groupname) {
        // no-op
    }

    @Override
    protected boolean mkOneDirWithMode(Path p, File p2f, FsPermission permission) throws IOException {
        return p2f.mkdir() || p2f.isDirectory();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        File file = pathToFile(f);
        if (!file.exists()) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        return toStatus(file, makeQualified(f));
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws IOException {
        return getFileStatus(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        File file = pathToFile(f);
        if (!file.exists()) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        if (file.isFile()) {
            return new FileStatus[] {getFileStatus(f)};
        }
        String[] names = file.list();
        if (names == null) {
            throw new IOException("Cannot list " + f);
        }
        FileStatus[] out = new FileStatus[names.length];
        for (int i = 0; i < names.length; i++) {
            Path child = new Path(f, names[i]);
            out[i] = toStatus(new File(file, names[i]), makeQualified(child));
        }
        return out;
    }

    private FileStatus toStatus(File file, Path qualified) {
        return new FileStatus(
                file.isDirectory() ? 0L : file.length(),
                file.isDirectory(),
                1,
                getDefaultBlockSize(qualified),
                file.lastModified(),
                0L,
                DEFAULT_PERMISSION,
                System.getProperty("user.name"),
                System.getProperty("user.name"),
                qualified);
    }
}
