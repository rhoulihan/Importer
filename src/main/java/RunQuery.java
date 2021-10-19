package com.amazonaws.Importer;

import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public class RunQuery implements Runnable {
	private QuerySpec spec;
	private boolean gsiQuery;
	private Integer shardId;
	
	public RunQuery(Integer shardId, QuerySpec spec, boolean gsiQuery) {
		synchronized (Main.sync) {
			Main.numThreads.incrementAndGet();
		}
		
		this.shardId = shardId;
		this.spec = spec;
		this.gsiQuery = gsiQuery;
	}

	@Override
	public void run() {
		int count = 0;
		ItemCollection<QueryOutcome> results;

		if (gsiQuery)
			results = Main.db.getTable(Main.target).getIndex("GSI1").query(spec);
		else
			results = Main.db.getTable(Main.target).query(spec);
		
		for (Page<Item, QueryOutcome> page : results.pages()) {
			Iterator<Item> it = page.iterator();
			while (it.hasNext()) {
				it.next();
				count++;
			}
		}
		
		synchronized (Main.sync) {
			Main.counts.put(shardId, Integer.valueOf(count));
			Main.numThreads.decrementAndGet();
		}
	}
}
