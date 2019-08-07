package capricorn.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description :
 * CREATE TEMPORARY FUNCTION regexp_extract_all AS 'capricorn.hive.udf.UdfRegexpExtractAll';
 * SELECT regexp_extract_all(name,'^[0-9]*$','\t') from users limit 1;
 * @Author : Capricorn.QBB
 * @Date : 2019-08-06
 * @Version : 1.0
 */
@Description(name = "regexp_extract_all",
        value = "_FUNC_(haystack, pattern, delimiter) - Find all the instances of pattern in haystack and return a string.")
public class UdfRegexpExtractAllRtnStr extends UDF {
    private String lastRegex = null;
    private Pattern pattern = null;

    public String evaluate(String input, String regex, String delimiter, String suffix) {
        if (input == null || regex == null || "".equals(regex.trim())) {
            return null;
        }

        if (!regex.equals(lastRegex)) {
            lastRegex = regex;
            pattern = Pattern.compile(regex, Pattern.MULTILINE);
        }

        StringBuffer sb = new StringBuffer();

        Matcher matcher = pattern.matcher(input);
        while (matcher.find()) {
            if (!"".equals(matcher.group(0))) {
                if (suffix != null) {
                    sb.append(delimiter).append(matcher.group(0)).append(suffix);
                } else {
                    sb.append(delimiter).append(matcher.group(0));
                }
            }
        }

        return sb.toString().replaceFirst(delimiter, "");
    }

    public String evaluate(String s, String regex, String suffix) {
        return this.evaluate(s, regex, ",", suffix);
    }

    public String evaluate(String s, String regex) {
        return this.evaluate(s, regex, ",", null);
    }

//	public static void main(String[] args) {
//		UdfRegexpExtractAll u = new UdfRegexpExtractAll();
//		String rs = u.evaluate("15913102833，111，421974539", "[1-9][0-9]{4,}", "@qq.com");
//		System.out.println(rs);
//		rs = u.evaluate("12123asldjfalkhf123zfhlsdk15913102833", "(?<!\\d)(?:(?:1[358]\\d{9})|(?:861[358]\\d{9}))(?!\\d)", "\t");
//		System.out.println(rs);
//	}
}
