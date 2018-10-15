package sdfs.util;

import java.nio.file.InvalidPathException;

public class FileUtil {
    /**
     * @param uri the file path
     * @return true if the path is valid else false
     */
    public static boolean isValidPath(String uri) {
        if (uri == null)
            throw new InvalidPathException("", "Path is NULL");
        else if (!uri.startsWith("/"))
            throw new InvalidPathException(uri, "File without starting slash");
        else if (uri.endsWith("/"))
            throw new InvalidPathException(uri, "File with ending slash.");
        return true;
    }

    public static String getName(String uri) {
        String tokens[] = uri.split("/");
        return tokens[tokens.length - 1];
    }

    /**
     * URI must start and end with a '/'.
     *
     * @param uri the directory path
     * @return true if the path is valid else false
     */
    public static boolean isValidDir(String uri) {
        if (uri == null)
            throw new InvalidPathException("", "Path is NULL");
        else if (!uri.startsWith("/"))
            throw new InvalidPathException(uri, "Directory without starting slash");
        else if (!uri.endsWith("/"))
            throw new InvalidPathException(uri, "Directory without ending slash.");
        return true;
    }
}
