package com.xuxueli.crawler.listener;

import com.xuxueli.crawler.model.PageRequest;

public interface ICrawlerListener {
	public void beforeProcessPage(PageRequest pageRequest);
	public void endProcessPage(PageRequest pageRequest,boolean result);
	public void onException(PageRequest pageRequest,Throwable e);

}
