package com.solgen.pa1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.grouplens.lenskit.cursors.Cursor;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.dao.PrefetchingUserEventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.Rating;
import org.grouplens.lenskit.data.history.UserHistory;
import org.grouplens.lenskit.data.pref.Preference;
import org.grouplens.lenskit.eval.data.CSVDataSource;
import org.grouplens.lenskit.eval.data.CSVDataSourceBuilder;

public class NonPersRecommender {

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter simpleWriter = null;
		PrintWriter advancedWriter = null;
		
		simpleWriter = new PrintWriter("pa1-result.txt", "UTF-8");
		advancedWriter = new PrintWriter("pa1-adv-result.txt", "UTF-8");
		
		DecimalFormat twoDForm = new DecimalFormat("#.00");
		
		Long inMovies[] = new Long[3];
		/*
		inMovies[0] = 11l;
		inMovies[1] = 121l;
		inMovies[2] = 8587l;
		*/
		///*
		inMovies[0] = 5503l;
		inMovies[1] = 107l;
		inMovies[2] = 1572l;
		//*/
		
		Map<Long,  Set<Long> > inMoviesRatingCount = new HashMap<Long,  Set<Long>>();
		//Map<Long, Integer> inMoviesNonRatingCount = new HashMap<Long, Integer>();
		
		Map<Long, Set<Long> > inMoviesNonRatingCount = new HashMap<Long, Set<Long> >();
		
		for (int i = 0; i < inMovies.length; i++) {
			inMoviesRatingCount.put(inMovies[i],new HashSet<Long>());
			inMoviesNonRatingCount.put(inMovies[i], new HashSet<Long>());
		}
		
		Map<Long, Set<Long>> inMoviesUsers = new HashMap<Long, Set<Long>>();
		
		Map<Long, HashMap<Long, Set<Long>>> otherMoviesRatingCount = new HashMap<Long, HashMap<Long, Set<Long>>>();
		Map<Long, HashMap<Long, Set<Long>>> advancedOtherMoviesRatingCount = new HashMap<Long, HashMap<Long, Set<Long>>>();
		
		CSVDataSourceBuilder csvBuilder = new CSVDataSourceBuilder(new File("data/recsys-data-ratings.csv"));
		CSVDataSource csv = csvBuilder.build();
		EventDAO eventDAO = csv.getEventDAO();
		PrefetchingUserEventDAO userDAO = new PrefetchingUserEventDAO(eventDAO);
		Cursor<UserHistory<Event>> users = userDAO.streamEventsByUser();
		
		for (UserHistory<Event> userHistory : users) {
			for (Event event : userHistory) {
				Preference preference = ((Rating)event).getPreference();				
				for (int i = 0; i < inMovies.length; i++) {
					if (preference.getItemId() == inMovies[i] && preference.getValue() != 0) {
						Set<Long> t =  inMoviesRatingCount.get(inMovies[i]);
						t.add(preference.getUserId());
						
						inMoviesRatingCount.put(inMovies[i], t);
						
						Set<Long> tmp = inMoviesUsers.get(inMovies[i]);
						if (tmp == null) {
							tmp = new HashSet<Long>();
						}
						
						tmp.add(preference.getUserId());
						inMoviesUsers.put(inMovies[i], tmp);
					} else {
						Set<Long> tt =  inMoviesNonRatingCount.get(inMovies[i]);
						tt.add(preference.getUserId());
						inMoviesNonRatingCount.put(inMovies[i], tt);
					}
				}
			}
		}
		
		
		for (int i = 0; i < inMovies.length; i++) {
			System.out.println(" ------- " + inMoviesRatingCount.get(inMovies[i]));
		}
		users.close();
		users = userDAO.streamEventsByUser();
		
		for (UserHistory<Event> userHistory : users) {
			for (Event event : userHistory) {
				Preference preference = ((Rating)event).getPreference();
				for (int i = 0; i < inMovies.length; i++) {
					if (inMoviesUsers.get(inMovies[i]).contains(preference.getUserId())){
						HashMap<Long,Set<Long>> tmp = otherMoviesRatingCount.get(inMovies[i]);
						if (tmp == null) {
							tmp = new HashMap<Long,Set<Long>>();
						}
						Set<Long> oldCount = tmp.get(preference.getItemId());
						if(oldCount == null) {
							oldCount = new HashSet<Long>();
						}
						oldCount.add(preference.getUserId());
						tmp.put(preference.getItemId(), oldCount);
						otherMoviesRatingCount.put(inMovies[i], tmp);
					} else {
						// calculating "not x and y"
						HashMap<Long,Set<Long>> advtmp = advancedOtherMoviesRatingCount.get(inMovies[i]);
						if (advtmp == null) {
							advtmp = new HashMap<Long,Set<Long>>();
						}
						Set<Long> oldCount = advtmp.get(preference.getItemId());
						if(oldCount == null) {
							oldCount = new HashSet<Long>();;
						}
						oldCount.add(preference.getUserId());
						advtmp.put(preference.getItemId(), oldCount);
						advancedOtherMoviesRatingCount.put(inMovies[i], advtmp);
					}
				}
			}
		}
		users.close();
		
		for (int i = 0; i < inMovies.length; i++) {
			HashMap<Long,Set<Long>> tmp = otherMoviesRatingCount.get(inMovies[i]);
			HashMap<Long,Set<Long>> advancedTmp = advancedOtherMoviesRatingCount.get(inMovies[i]);
			
			Double x = inMoviesRatingCount.get(inMovies[i]).size()*1.0;
			Double notx = 5564- x;//inMoviesNonRatingCount.get(inMovies[i]).size()*1.0;
			
			Map<Long, Double> simpleResUnsorted = new HashMap<Long, Double>();
			Map<Long, Double> advancedResUnsorted = new HashMap<Long, Double>();
			
			for (Map.Entry<Long, Set<Long>> entry : tmp.entrySet()) {
			    Long key = entry.getKey();
			    
			    if (key.longValue() == inMovies[i].longValue()) continue;
			    
			    Double xandy = entry.getValue().size()*1.0;			    
			    Double simpleRes = xandy/x;
			    simpleResUnsorted.put(key, simpleRes);
			}
			
			for (Map.Entry<Long, Set<Long>> entry : advancedTmp.entrySet()) {
			    Long key = entry.getKey();
			    
			    if (key.longValue() == inMovies[i].longValue()) continue;
			    
			    Double simpleRes = simpleResUnsorted.get(key);
			    Double notxandy = entry.getValue().size()*1.0;
			    Double advancedRes = (simpleRes/(notxandy/notx));
			    
			    //System.out.println(inMoviesNonRatingCount.get(inMovies[i]) +  " - "+ x + " = " + (inMoviesNonRatingCount.get(inMovies[i]) - x));
			    //
			    System.out.println(key.longValue() + " --- "  + advancedRes);
			    advancedResUnsorted.put(key, advancedRes);
			}
			
			Map<Long, Double> simpleSortedMap = sortByComparator(simpleResUnsorted);
			Map<Long, Double> advancedSortedMap = sortByComparator(advancedResUnsorted);
			
			simpleWriter.print(inMovies[i] + ",");
			
			int j = 0; 
			
			for (Map.Entry<Long, Double> entry : simpleSortedMap.entrySet()) {
				simpleWriter.print(entry.getKey() + "," + Double.valueOf(twoDForm.format(entry.getValue())));
				
				if(++j >= 5){
					simpleWriter.println();
					break;
				} else {
					simpleWriter.print(",");
				}
			}
			
			advancedWriter.print(inMovies[i] + ",");
			j = 0;
			for (Map.Entry<Long, Double> entry : advancedSortedMap.entrySet()) {
				advancedWriter.print(entry.getKey() + "," + Double.valueOf(twoDForm.format(entry.getValue())));
				
				if(++j >= 5){
					advancedWriter.println();
					break;
				} else {
					advancedWriter.print(",");
				}
			}
		}
		
		System.out.println("The End is Come ...");
		
		simpleWriter.close();
		advancedWriter.close();
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	private static Map<Long, Double> sortByComparator(Map<Long, Double> unsortMap) {
		
		List list = new LinkedList(unsortMap.entrySet());
 
		// sort list based on comparator
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return -(((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue()));
			}
		});
 
		// put sorted list into map again
		//LinkedHashMap make sure order in which keys were inserted
		Map<Long, Double> sortedMap = new LinkedHashMap<Long, Double>();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry<Long, Double> entry = (Map.Entry<Long, Double>) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
