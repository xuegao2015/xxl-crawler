package com.xuxueli.crawler.rundata;

import java.util.Map;

/**
 * run data
 *
 * @author xuxueli 2017-12-14 11:40:50
 */
public interface IMonitorableRunData {
	public enum FailReason{
		TimeOut,
		ConnectExc,
		IOExc,
		Code503,
		Code404,
		Other
	}
	
	/**
     * add link
     *
     * @param link
     * @return boolean
     */
    public abstract boolean addUrl(String link);

    /**
     * get link, remove from unVisitedUrlQueue and add to visitedUrlSet
     *
     * @return String
     */
    public abstract String getUrl();

    /**
     * get url num
     *
     * @return int
     */
    public abstract int getUrlNum();

    /**
     * add link
     *
     * @param link
     * @return boolean
     */
    public boolean addFailedUrl(FailReason reasone,String link);

    /**
     * get link, remove from unVisitedUrlQueue and add to visitedUrlSet
     *
     * @return String
     */
    public String getFailedUrl(FailReason reasone);

    /**
     * get url num
     *
     * @return int
     */
    public int getFailedUrlNum(FailReason reasone);
    
    /**
     * get url num
     *
     * @return int
     */
    public int getFailedUrlNum();
    
    /**
     * 查看当前运行状态
     * @return
     */
    public Map<String, Object> getStatus();

}
