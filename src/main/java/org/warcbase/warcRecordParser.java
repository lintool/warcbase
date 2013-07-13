
public class warcRecordParser {
	public static String getType(String record){
		String lines[] = record.split("\\r?\\n");
		for(String line: lines){
			if(line.startsWith("Content-Type")){
				return line.substring(14).split(";")[0];
			}
		}
		return "";
	}
	
	public static String getBody(String record){
		String body = record;
		while(!body.startsWith("\n"))
			body = body.substring(body.indexOf('\n')+1);
		body = body.substring(body.indexOf('\n')+1);
		return body;
	}
	
	public static void main(String[] args) {
		String test = new String("Content-Type: text/html; charset=UTF-8");
		System.out.println(test.substring(14).split(";")[0]);
	}
}
