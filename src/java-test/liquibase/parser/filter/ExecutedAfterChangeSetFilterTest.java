package liquibase.parser.filter;

import liquibase.ChangeSet;
import liquibase.RanChangeSet;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;

public class ExecutedAfterChangeSetFilterTest {

    @Test
    public void accepts_noRan() throws Exception {
        ExecutedAfterChangeSetFilter filter = new ExecutedAfterChangeSetFilter(new Date(), new ArrayList<RanChangeSet>());

        assertFalse(filter.accepts(new ChangeSet("1", "testAuthor", false, false, "path/changelog",null,  null, null)));
    }

    @Test
    public void accepts_nullDate() throws Exception {
        ArrayList<RanChangeSet> ranChanges = new ArrayList<RanChangeSet>();
        ranChanges.add(new RanChangeSet("path/changelog", "1", "testAuthor", "12345", new Date(), null));
        ranChanges.add(new RanChangeSet("path/changelog", "2", "testAuthor", "12345", null, null));
        ranChanges.add(new RanChangeSet("path/changelog", "3", "testAuthor", "12345", new Date(), null));
        ExecutedAfterChangeSetFilter filter = new ExecutedAfterChangeSetFilter(new Date(), ranChanges);

        assertFalse(filter.accepts(new ChangeSet("1", "testAuthor", false, false, "path/changelog", null, null, null)));
    }

    @Test
    public void accepts() throws Exception {
        ArrayList<RanChangeSet> ranChanges = new ArrayList<RanChangeSet>();
        ranChanges.add(new RanChangeSet("path/changelog", "1", "testAuthor", "12345", new Date(new Date().getTime() - 10*1000*60*60), null));
        ranChanges.add(new RanChangeSet("path/changelog", "2", "testAuthor", "12345", new Date(new Date().getTime() - 8*1000*60*60), null));
        ranChanges.add(new RanChangeSet("path/changelog", "3", "testAuthor", "12345", new Date(new Date().getTime() - 4*1000*60*60), null));
        ExecutedAfterChangeSetFilter filter = new ExecutedAfterChangeSetFilter(new Date(new Date().getTime() - 6*1000*60*60), ranChanges);

        assertFalse(filter.accepts(new ChangeSet("1", "testAuthor", false, false, "path/changelog",null,  null, null)));
        assertFalse(filter.accepts(new ChangeSet("2", "testAuthor", false, false, "path/changelog",null,  null, null)));
        assertTrue(filter.accepts(new ChangeSet("3", "testAuthor", false, false, "path/changelog", null, null, null)));

    }
}
