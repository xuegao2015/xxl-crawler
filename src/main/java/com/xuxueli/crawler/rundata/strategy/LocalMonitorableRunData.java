package com.xuxueli.crawler.rundata.strategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xuxueli.crawler.exception.XxlCrawlerException;
import com.xuxueli.crawler.rundata.IMonitorableRunData;
import com.xuxueli.crawler.rundata.RunData;
import com.xuxueli.crawler.util.UrlUtil;

/**
 * lcoal run data
 *
 * @author xuxueli 2017-12-14 11:42:23
 */
public class LocalMonitorableRunData implements IMonitorableRunData {
    private static Logger logger = LoggerFactory.getLogger(LocalMonitorableRunData.class);

    // url
    private volatile LinkedBlockingQueue<String> unVisitedUrlQueue = new LinkedBlockingQueue<String>();     // 待采集URL池
    private volatile Set<String> visitedUrlSet = Collections.synchronizedSet(new HashSet<String>());        // 已采集URL池

    // url
    private volatile ConcurrentHashMap<FailReason,LinkedBlockingQueue<String>> failedUrlMap = new ConcurrentHashMap<FailReason,LinkedBlockingQueue<String>>();     // 采集失败URL池
    

    public LocalMonitorableRunData(){
    	for(FailReason reason:FailReason.values()){
    		failedUrlMap.put(reason, new LinkedBlockingQueue<String>());
    	}
    }
    
    /**
     * url add
     * @param link
     */
    public boolean addUrl(String link) {
        if (!UrlUtil.isUrl(link)) {
            logger.debug(">>>>>>>>>>> xxl-crawler addUrl fail, link not valid: {}", link);
            return false; // check URL格式
        }
        if (visitedUrlSet.contains(link)) {
            logger.debug(">>>>>>>>>>> xxl-crawler addUrl fail, link repeate: {}", link);
            return false; // check 未访问过
        }
        if (unVisitedUrlQueue.contains(link)) {
            logger.debug(">>>>>>>>>>> xxl-crawler addUrl fail, link visited: {}", link);
            return false; // check 未记录过
        }
        unVisitedUrlQueue.add(link);
        logger.info(">>>>>>>>>>> xxl-crawler addUrl success, link: {}", link);
        return true;
    }

    /**
     * url take
     * @return String
     * @throws InterruptedException
     */
    public String getUrl() {
        String link = null;
        try {
            link = unVisitedUrlQueue.take();
        } catch (InterruptedException e) {
            throw new XxlCrawlerException("LocalRunData.getUrl interrupted.");
        }
        if (link != null) {
            visitedUrlSet.add(link);
        }
        return link;
    }

    public int getUrlNum() {
        return unVisitedUrlQueue.size();
    }

	@Override
	public boolean addFailedUrl(FailReason resone, String link) {
		LinkedBlockingQueue<String> failed = failedUrlMap.get(resone);
		synchronized (failed) {
			failed.add(link);
		}
		return false;
	}

	@Override
	public String getFailedUrl(FailReason reasone) {
		LinkedBlockingQueue<String> failed = failedUrlMap.get(reasone);
		synchronized (failed) {
			try {
	            return failed.take();
	        } catch (InterruptedException e) {
	            throw new XxlCrawlerException("LocalRunData.getUrl interrupted.");
	        }
		}
	}

	@Override
	public int getFailedUrlNum(FailReason reasone) {
		LinkedBlockingQueue<String> failed = failedUrlMap.get(reasone);
		return failed.size();
	}
	
	@Override
	public int getFailedUrlNum() {
		final AtomicInteger count = new AtomicInteger(0);
		failedUrlMap.forEach((k,v)->{
			count.set(count.get()+v.size());
		});
		return count.get();
	}

	@Override
	public Map<String, Object> getStatus() {
		Map<String,Object> status = new HashMap<>();
		status.put("UnVisitedUrl", unVisitedUrlQueue.size());
		status.put("VisitedUrl", visitedUrlSet.size());
		status.put("FailedUrl", getFailedUrlNum());
		return status;
	}

}
