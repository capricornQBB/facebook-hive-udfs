package capricorn.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description :
 * CREATE TEMPORARY FUNCTION regexp_extract_all AS 'capricorn.hive.udf.UdfRegexpExtractAll';
 * SELECT regexp_extract_all(name,'^[0-9]*$','\t') from users limit 1;
 * @Author : Capricorn.QBB
 * @Date : 2019-08-07
 * @Version : 1.0
 */
@Description(name = "regexp_extract_all",
		value = "_FUNC_(haystack, pattern, delimiter) - Find all the instances of pattern in haystack")
public class UdfRegexpExtractAll extends UDF {

	private String lastRegex = null;
	private Pattern pattern = null;

	public List<String> evaluate(String s, String regex, Integer extractIndex, String appendStr) {
		if (s == null || regex == null || extractIndex == null) {
			return null;
		}
		if (!regex.equals(lastRegex)) {
			lastRegex = regex;
			pattern = Pattern.compile(regex, Pattern.MULTILINE);
		}
		LinkedList<String> result = new LinkedList<>();
		Matcher matcher = pattern.matcher(s);
		while (matcher.find()) {
			MatchResult mr = matcher.toMatchResult();
			if (!"".equals(matcher.group(extractIndex))) {
				if (appendStr != null) {
					result.add(mr.group(extractIndex) + appendStr);
				} else {
					result.add(mr.group(extractIndex));
				}
			}
		}
		return result;
	}

	public List<String> evaluate(String s, String regex) {
		return this.evaluate(s, regex, 0, null);
	}

	public List<String> evaluate(String s, String regex, String appendStr) {
		return this.evaluate(s, regex, 0, appendStr);
	}
}
