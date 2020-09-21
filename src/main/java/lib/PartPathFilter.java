package lib;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class PartPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        return path.getName().startsWith("part");
    }
}
