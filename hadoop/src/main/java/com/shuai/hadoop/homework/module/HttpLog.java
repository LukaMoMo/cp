package com.shuai.hadoop.homework.module;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.*;

public class HttpLog {
	private Integer respCode;
	private String reqType;
	private long respSize;
	private String Uri;
	private String refer;
	private String userAgent;
	private String Time;
	private boolean isVaild;
	private String ip;

	public static final Pattern httpLogPatter = Pattern
			.compile("([^\\s]+) - - \\[(.+)\\] \"([^\\s]+) ([^\\s]+) HTTP/([^\\s]+)\" ([0-9]+) ([0-9]+) \"([^\\s]*)\" \"(.*)\"",Pattern.CASE_INSENSITIVE);
	public static final Pattern referDomain = Pattern
			.compile("(?=((.*://)?))[^/\\:]+(?=((:.*)|(.*)))",Pattern.CASE_INSENSITIVE);

	public HttpLog(String lineString) {
		// System.out.println(httpLogMatcher.group(i));
		Matcher httpLogMatcher = httpLogPatter.matcher(lineString.toString());
		if (httpLogMatcher.matches()) {
			this.ip = httpLogMatcher.group(1);
			this.Time = httpLogMatcher.group(2);
			this.reqType = httpLogMatcher.group(3);
			this.Uri = httpLogMatcher.group(4);

			this.respCode = Integer.parseInt(httpLogMatcher.group(6));
			this.respSize = Integer.parseInt(httpLogMatcher.group(7));
			this.refer = httpLogMatcher.group(8);
			this.userAgent = httpLogMatcher.group(9);

			if (checkVaild()) {
				setIsVaild(true);
			} else {
				setIsVaild(false);
			}
		} else {
			setIsVaild(false);
		}
	}

	public Integer getRespCode() {
		return respCode;
	}

	public void setRespCode(Integer respCode) {
		this.respCode = respCode;
	}

	public String getReqType() {
		return reqType;
	}

	public void setReqType(String reqType) {
		this.reqType = reqType;
	}

	public long getRespSize() {
		return respSize;
	}

	public void setRespSize(long respSize) {
		this.respSize = respSize;
	}

	public String getUri() {
		return Uri;
	}

	public void setUri(String uri) {
		Uri = uri;
	}

	public String getRefer() {
		return refer;
	}

	public void setRefer(String refer) {
		this.refer = refer;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public String getTime() {
		return Time;
	}

	public void setTime(String time) {
		Time = time;
	}

	public boolean getIsVaild() {
		return isVaild;
	}

	public void setIsVaild(boolean isVaild) {
		this.isVaild = isVaild;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getDomain() {
		String domain = getRefer().replaceFirst("\\?.*", "").replaceFirst(".*://", "");
		if(domain == null || domain.length() < 3) {
			return "NO_REFER";
		}
		Matcher httpLogMatcher = referDomain.matcher(domain);
		if (httpLogMatcher.find()) {
			return httpLogMatcher.group();
		} else {
			domain.replace("((.*//)|((:[0-9]*)*/.*))", "");
			return "NO_Matcher" + domain;
		}

	}

	private boolean checkUrlVaild() {
		Set<String> pages = new HashSet<String>();
		pages.add("/about");
		pages.add("/black-ip-list/");
		pages.add("/cassandra-clustor/");
		pages.add("/finance-rhive-repurchase/");
		pages.add("/hadoop-family-roadmap/");
		pages.add("/hadoop-hive-intro/");
		pages.add("/hadoop-zookeeper-intro/");
		pages.add("/hadoop-mahout-roadmap/");

		if (pages.contains(this.getUri())) {
			return false;
		}
		return true;
	}

	private boolean checkRespVaild() {
		if (this.getRespCode() > 400 || this.getRespSize() < 100) {
			return false;
		}

		return true;
	}

	private boolean checkUserAgentVaild() {

		if (this.getUserAgent() == null) {
			return false;
		}

		return true;
	}

	public boolean checkVaild() {
		if (checkUserAgentVaild() && checkRespVaild() && checkUrlVaild()) {
			return true;
		}
		return false;

	}

}
