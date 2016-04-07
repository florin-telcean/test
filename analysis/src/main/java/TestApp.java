import com.adforms.interviewtest.analysis.Lastfm;
import com.adforms.interviewtest.commons.Recommendations;

/**
 * Test application used to debug different parts of the API
 */
public class TestApp {
    public static void main(String[] args) {
        Lastfm api = new Lastfm("lastfmservice/dataset/2user.tsv", "lastfmservice/dataset/userid-profile.tsv", "local[*]");
        Recommendations rec = api.recommend("user_00001", 2);
        System.out.println(rec.toString());
    }
}
