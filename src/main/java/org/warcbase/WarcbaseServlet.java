package org.warcbase;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class WarcbaseServlet extends HttpServlet
{

	private void writeResponse(HttpServletResponse resp, byte[] data, String query, String d) throws IOException{
		String content = new String(data, "UTF8");
		//System.out.println(content);
		
		if(!warcRecordParser.getType(content).startsWith("text")){
			resp.setHeader("Content-Type", ResponseRecord.getType(content));
			resp.setContentLength(ResponseRecord.getBodyByte(data).length);
			resp.getOutputStream().write(ResponseRecord.getBodyByte(data));
			//resp.getOutputStream().write(ResponseRecord.getBodyByte(data));
    		//IOUtils.write(ResponseRecord.getBodyByte(data), resp.getOutputStream());
		}
		else{
		  System.setProperty("file.encoding", "UTF8");
		  //resp.setContentType("text/html;charset=UTF-8");
		  resp.setHeader("Content-Type", ResponseRecord.getType(content));
		  resp.setCharacterEncoding("UTF-8");
			PrintWriter out = resp.getWriter();
			//out.
			TextDocument2 t2 = new TextDocument2(null, null, null);
			String bodyContent = new String(ResponseRecord.getBodyByte(data), "UTF8");
			bodyContent = t2.fixURLs(bodyContent, query, d);
			//System.out.println(bodyContent);
			out.println(bodyContent);
			//resp.getOutputStream().write(bodyContent.getBytes("UTF-8"));
		}
	}
	
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException
    {
        String query = req.getParameter("query");
        String d = req.getParameter("date");
   
        String q = Util.reverse_hostname(query);
        Configuration config = HBaseConfiguration.create();
    	HTable table = new HTable(LoadWARC.hbaseConfig, Constants.TABLE_NAME);
    	Get get = new Get(Bytes.toBytes(q));
        Result rs = table.get(get);
        byte[] data = null;
        String imagePath = null;
        File imageFile = null;
    	
        if(rs.raw().length == 0){
        	PrintWriter out = resp.getWriter();
        	out.println("Not Found.");
        	table.close();
        	return;
        }
        if(d != null){
        	for(int i=0;i<rs.raw().length;i++){
        		String date = new String(rs.raw()[i].getQualifier());
        		if(date.equals(d)){
        			data = rs.raw()[i].getValue();
        			writeResponse(resp, data, query, d);
        			/*String content = new String(rs.raw()[i].getValue());
        			if(!warcRecordParser.getType(content).startsWith("text")){
        				resp.setHeader("Content-Type", ResponseRecord.getType(content));
        				resp.setContentLength(ResponseRecord.getBodyByte(data).length);
        				resp.getOutputStream().write(ResponseRecord.getBodyByte(data));
        				//resp.getOutputStream().write(ResponseRecord.getBodyByte(data));
			    		//IOUtils.write(ResponseRecord.getBodyByte(data), resp.getOutputStream());
        			}
        			else{
        				PrintWriter out = resp.getWriter();
        				TextDocument2 t2 = new TextDocument2(null, null, null);
            			out.println(t2.fixURLs(new String(ResponseRecord.getBodyByte(data)), query, d));
        			}*/
        			//out.close();
        			table.close();
        			return;
        		}
        	}
        	ArrayList<String> dates = new ArrayList<String>(10);
            for(int i=0;i<rs.raw().length;i++)
            	dates.add(new String(rs.raw()[i].getQualifier()));
            Collections.sort(dates);
            for(int i=1;i<dates.size();i++)
            	if(dates.get(i).compareTo(d) > 0){//d < i
        			data = rs.raw()[i - 1].getValue();
        			writeResponse(resp, data, query, d);
        			/*String content = new String(rs.raw()[i - 1].getValue());
        			if(!warcRecordParser.getType(content).equals("text/html")){
        				resp.setHeader("Content-Type", "image/jpg");
        				resp.setContentLength(ResponseRecord.getBodyByte(data).length);
        				resp.getOutputStream().write(ResponseRecord.getBodyByte(data));
        			}
        			else{
        				PrintWriter out = resp.getWriter();
        				TextDocument2 t2 = new TextDocument2(null, null, null);
            			out.println(t2.fixURLs(ResponseRecord.getBodyText(content), query, d));
        			}
        			//out.close();
        			table.close();*/
        			return;
            	}
            int i = dates.size();
            data = rs.raw()[i - 1].getValue();
            writeResponse(resp, data, query, d);
			/*String content = new String(rs.raw()[i - 1].getValue());
			if(!warcRecordParser.getType(content).equals("text/html")){
				resp.setHeader("Content-Type", "image/jpg");
				resp.setContentLength(ResponseRecord.getBodyByte(data).length);
				resp.getOutputStream().write(ResponseRecord.getBodyByte(data));
			}
			else{
				PrintWriter out = resp.getWriter();
				TextDocument2 t2 = new TextDocument2(null, null, null);
    			out.println(t2.fixURLs(ResponseRecord.getBodyText(content), query, d));
			}
			//out.close();
			table.close();*/
			return;
            		
        	//return;
        }
        PrintWriter out = resp.getWriter();
        out.println("<html>");
        out.println("<body>");
        for(int i=0;i<rs.raw().length;i++){
        	String date = new String(rs.raw()[i].getQualifier());
        	out.println("<br/> <a href='http://"  + req.getServerName() + ":" + req.getServerPort() + req.getRequestURI() + "?query=" + req.getParameter("query") + "&date=" + date + "'>" + date + "</a>");//<a href="url">Link text</a>
        }
        out.println("</body>");
        out.println("</html>");
        table.close();
    }

    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException
    {
        String field = req.getParameter("field");
        PrintWriter out = resp.getWriter();

        out.println("<html>");
        out.println("<body>");
        out.println("You entered \"" + field + "\" into the text box.");
        out.println("</body>");
        out.println("</html>");
    }
}
