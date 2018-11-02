package com.xuxueli.crawler;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xg.framework.thread.MonitorableThreadFactoryBuilder;
import com.xg.framework.thread.MonitorableThreadPoolExecutor;
import com.xg.framework.thread.XgExecutors;
import com.xuxueli.crawler.listener.ICrawlerListener;
import com.xuxueli.crawler.loader.PageLoader;
import com.xuxueli.crawler.model.PageRequest;
import com.xuxueli.crawler.model.RunConf;
import com.xuxueli.crawler.parser.PageParser;
import com.xuxueli.crawler.proxy.ProxyMaker;
import com.xuxueli.crawler.rundata.IMonitorableRunData;
import com.xuxueli.crawler.rundata.IMonitorableRunData.FailReason;
import com.xuxueli.crawler.rundata.strategy.LocalMonitorableRunData;
import com.xuxueli.crawler.thread.MonitorableCrawlerThread;

public class MonitorableCrawler implements ICrawlerListener{
	/**
	 * Crawler name
	 */
	String name;
	private static Logger logger = LoggerFactory.getLogger(MonitorableCrawler.class);
	private List<MonitorableCrawlerThread> crawlerThreads = new CopyOnWriteArrayList<MonitorableCrawlerThread>();
	List<ICrawlerListener> listeners;
	ExecutorService crawlers;
	
	// thread
    int threadCount = 1;      
	// run conf
    volatile RunConf runConf = new RunConf(); 
    IMonitorableRunData runData = new LocalMonitorableRunData();
	
	
	public static class MBuilder{
		MonitorableCrawler crawler = new MonitorableCrawler();
		
		// run data
        /**
         * 设置运行数据类型
         *
         * @param runData
         * @return Builder
         */
        public MBuilder setName(String name){
        	crawler.setName(name);
            return this;
        }
		
		// run data
        /**
         * 设置运行数据类型
         *
         * @param runData
         * @return Builder
         */
        public MBuilder addListener(ICrawlerListener listener){
        	crawler.addListeners(listener);
            return this;
        }
        

        // run data
        /**
         * 设置运行数据类型
         *
         * @param runData
         * @return Builder
         */
        public MBuilder setRunData(LocalMonitorableRunData runData){
            crawler.runData = runData;
            return this;
        }

        /**
         * 待爬的URL列表
         *
         * @param urls
         * @return MBuilder
         */
        public MBuilder setUrls(String... urls) {
            if (urls!=null && urls.length>0) {
                for (String url: urls) {
                    crawler.runData.addUrl(url);
                }
            }
            return this;
        }

        // run conf
        /**
         * 允许扩散爬取，将会以现有URL为起点扩散爬取整站
         *
         * @param allowSpread
         * @return MBuilder
         */
        public MBuilder setAllowSpread(boolean allowSpread) {
            crawler.runConf.setAllowSpread(allowSpread);
            return this;
        }

        /**
         * URL白名单正则，非空时进行URL白名单过滤页面
         *
         * @param whiteUrlRegexs
         * @return MBuilder
         */
        public MBuilder setWhiteUrlRegexs(String... whiteUrlRegexs) {
            if (whiteUrlRegexs!=null && whiteUrlRegexs.length>0) {
                for (String whiteUrlRegex: whiteUrlRegexs) {
                    crawler.runConf.getWhiteUrlRegexs().add(whiteUrlRegex);
                }
            }
            return this;
        }

        /**
         * 页面解析器
         *
         * @param pageParser
         * @return MBuilder
         */
        public MBuilder setPageParser(PageParser pageParser){
            crawler.runConf.setPageParser(pageParser);
            return this;
        }

        /**
         * 页面下载器
         *
         * @param pageLoader
         * @return MBuilder
         */
        public MBuilder setPageLoader(PageLoader pageLoader){
            crawler.runConf.setPageLoader(pageLoader);
            return this;
        }

        // site
        /**
         * 请求参数
         *
         * @param paramMap
         * @return MBuilder
         */
        public MBuilder setParamMap(Map<String, String> paramMap){
            crawler.runConf.setParamMap(paramMap);
            return this;
        }

        /**
         * 请求Cookie
         *
         * @param cookieMap
         * @return MBuilder
         */
        public MBuilder setCookieMap(Map<String, String> cookieMap){
            crawler.runConf.setCookieMap(cookieMap);
            return this;
        }

        /**
         * 请求Header
         *
         * @param headerMap
         * @return MBuilder
         */
        public MBuilder setHeaderMap(Map<String, String> headerMap){
            crawler.runConf.setHeaderMap(headerMap);
            return this;
        }

        /**
         * 请求UserAgent
         *
         * @param userAgents
         * @return MBuilder
         */
        public MBuilder setUserAgent(String... userAgents){
            if (userAgents!=null && userAgents.length>0) {
                for (String userAgent: userAgents) {
                    if (!crawler.runConf.getUserAgentList().contains(userAgent)) {
                        crawler.runConf.getUserAgentList().add(userAgent);
                    }
                }
            }
            return this;
        }

        /**
         * 请求Referrer
         *
         * @param referrer
         * @return MBuilder
         */
        public MBuilder setReferrer(String referrer){
            crawler.runConf.setReferrer(referrer);
            return this;
        }

        /**
         * 请求方式：true=POST请求、false=GET请求
         *
         * @param ifPost
         * @return MBuilder
         */
        public MBuilder setIfPost(boolean ifPost){
            crawler.runConf.setIfPost(ifPost);
            return this;
        }

        /**
         * 超时时间，毫秒
         *
         * @param timeoutMillis
         * @return MBuilder
         */
        public MBuilder setTimeoutMillis(int timeoutMillis){
            crawler.runConf.setTimeoutMillis(timeoutMillis);
            return this;
        }

        /**
         * 停顿时间，爬虫线程处理完页面之后进行主动停顿，避免过于频繁被拦截；
         *
         * @param pauseMillis
         * @return MBuilder
         */
        public MBuilder setPauseMillis(int pauseMillis){
            crawler.runConf.setPauseMillis(pauseMillis);
            return this;
        }

        /**
         * 代理生成器
         *
         * @param proxyMaker
         * @return MBuilder
         */
        public MBuilder setProxyMaker(ProxyMaker proxyMaker){
            crawler.runConf.setProxyMaker(proxyMaker);
            return this;
        }

        /**
         * 失败重试次数，大于零时生效
         *
         * @param failRetryCount
         * @return MBuilder
         */
        public MBuilder setFailRetryCount(int failRetryCount){
            if (failRetryCount > 0) {
                crawler.runConf.setFailRetryCount(failRetryCount);
            }
            return this;
        }

        // thread
        /**
         * 爬虫并发线程数
         *
         * @param threadCount
         * @return MBuilder
         */
        public MBuilder setThreadCount(int threadCount) {
            crawler.threadCount = threadCount;
            return this;
        }

		public MonitorableCrawler build() {
			crawler.crawlers = XgExecutors.newCachedThreadPool(new MonitorableThreadFactoryBuilder(crawler.name));
            return crawler;
        }
	}
	
	public IMonitorableRunData getRunData() {
        return runData;
    }

    public RunConf getRunConf() {
        return runConf;
    }
	
	/**
     * 启动
     *
     * @param sync  true=同步方式、false=异步方式
     */
	
    public void start(boolean sync){
        if (runData == null) {
            throw new RuntimeException("xxl crawler runData can not be null.");
        }
        if (runData.getUrlNum() <= 0) {
            throw new RuntimeException("xxl crawler indexUrl can not be empty.");
        }
        if (runConf == null) {
            throw new RuntimeException("xxl crawler runConf can not be empty.");
        }
        if (threadCount<1 || threadCount>1000) {
            throw new RuntimeException("xxl crawler threadCount invalid, threadCount : " + threadCount);
        }
        if (runConf.getPageLoader() == null) {
            throw new RuntimeException("xxl crawler pageLoader can not be null.");
        }
        if (runConf.getPageParser() == null) {
            throw new RuntimeException("xxl crawler pageParser can not be null.");
        }

        logger.info(">>>>>>>>>>> {} crawler start ...",name);
        for (int i = 0; i < threadCount; i++) {
        	MonitorableCrawlerThread crawlerThread = new MonitorableCrawlerThread(this,listeners);
            crawlerThreads.add(crawlerThread);
        }
        for (MonitorableCrawlerThread crawlerThread: crawlerThreads) {
            crawlers.execute(crawlerThread);
        }
        crawlers.shutdown();

        if (sync) {
            try {
                while (!crawlers.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.info(">>>>>>>>>>> {} crawler still running ...",name);
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
    
    
    
    public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}

	public Map<String,Object> getStatus(){
		MonitorableThreadPoolExecutor executor = (MonitorableThreadPoolExecutor)crawlers;
		HashMap<String,Object> status = new HashMap<>();
		status.put("CrawlerName", name);
		status.put("ActiveCount", executor.getActiveCount());
		status.put("CompletedTaskCount", executor.getCompletedTaskCount());
		status.put("CorePoolSize", executor.getCorePoolSize());
		status.put("LargestPoolSize", executor.getLargestPoolSize());
		status.put("MaximumPoolSize", executor.getMaximumPoolSize());
		status.put("PoolSize", executor.getPoolSize());
		status.put("TaskCount", executor.getTaskCount());
		status.put("TaskCount", executor.getKeepAliveTime(TimeUnit.MINUTES)+" M");
		status.putAll(runData.getStatus());
		return status;
	}

	public List<ICrawlerListener> getListeners() {
		return listeners;
	}

	public void setListeners(List<ICrawlerListener> listeners) {
		this.listeners = listeners;
	}
	
	public void addListeners(ICrawlerListener listener) {
		if(listener==null)return;
		if(this.listeners==null){
			this.listeners = new ArrayList<>(); 
		}
		this.listeners.add(listener);
	}

	@Override
	public void beforeProcessPage(PageRequest pageRequest) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endProcessPage(PageRequest pageRequest, boolean result) {
		if(!result){
			logger.warn("failed to load page:{}",pageRequest.getUrl());
		}
	}

	@Override
	public void onException(PageRequest pageRequest, Throwable e) {
		if(pageRequest!=null){
			//java.net.SocketTimeoutException: Read timed out
			//java.net.ConnectException: Connection refused: connect
			//java.io.IOException: Unable to tunnel through proxy. Proxy returns "HTTP/1.1 503 Too many open connections"
			
			if(e instanceof SocketTimeoutException){
				runData.addFailedUrl(FailReason.TimeOut, pageRequest.getUrl());
			}else if(e instanceof ConnectException){
				runData.addFailedUrl(FailReason.ConnectExc, pageRequest.getUrl());
			}else if(e instanceof IOException){
				runData.addFailedUrl(FailReason.IOExc, pageRequest.getUrl());
			}else{
				runData.addFailedUrl(FailReason.Other, pageRequest.getUrl());
			}
		}
		
	}
	
	 /**
     * 尝试终止
     */
    public void tryFinish(){
        boolean isRunning = false;
        for (MonitorableCrawlerThread crawlerThread: crawlerThreads) {
            if (crawlerThread.isRunning()) {
                isRunning = true;
                break;
            }
        }
        boolean isEnd = runData.getUrlNum()==0 && !isRunning;
        if (isEnd) {
            logger.info(">>>>>>>>>>> {} crawler is finished.",name);
            stop();
        }
    }

    /**
     * 终止
     */
    public void stop(){
        for (MonitorableCrawlerThread crawlerThread: crawlerThreads) {
            crawlerThread.toStop();
        }
        crawlers.shutdownNow();
        logger.info(">>>>>>>>>>> {} crawler stop.",name);
    }
	
}
