#include "scheduler.h"

#include <CkSpider.h>

#include <algorithm>
#include <vector>
#include <unordered_map>
#include <queue>
#include <string>
#include <string.h>
#include <chrono>
#include <iostream>
#include <fstream>
#include <pthread.h>

#define LIMIT_URLS 100000
#define NUM_THREADS 40
#define WAITING_TIME_MS 100
#define MAX_WRITE_BUFFER_SZ 100
#define FIELD_SEPARATOR "|"

using namespace std;

Scheduler s;
pthread_mutex_t mutex;
int count_fetched;

// variables to generate the report
int count_fetching_error;
int count_fetched_lv1;
size_t total_size;
double total_time;
chrono::time_point<std::chrono::system_clock> g_start;
ofstream report_output;

string get_rowString(string url, string title, string html) {
	string fs = string(FIELD_SEPARATOR);
	replace(url.begin(), url.end(), '|', ' ');
	replace(url.begin(), url.end(), '\n', ' ');
	replace(url.begin(), url.end(), '\r', ' ');
	replace(title.begin(), title.end(), '|', ' ');
	replace(title.begin(), title.end(), '\n', ' ');
	replace(title.begin(), title.end(), '\r', ' ');
	replace(html.begin(), html.end(), '|', ' ');
	replace(html.begin(), html.end(), '\n', ' ');
	replace(html.begin(), html.end(), '\r', ' ');
	// return url + fs + title + string("\n");
	return url + fs + title + fs + html + string("\n");
}

void report() {
	//  total domains, total fetched, total fetched lv 1, total elapsed time, total fetching time in sec, total size in bytes, total of fetching errors, num of busy domains, num of new domains, num of idle domains 
	chrono::time_point<std::chrono::system_clock> end = chrono::system_clock::now(); 
	chrono::duration<double> elapsed_seconds = end - g_start; 

	string fs = string(FIELD_SEPARATOR);

	string str = to_string(s.get_numDomains()) + fs;
	str += to_string(count_fetched) + fs;
	str += to_string(count_fetched_lv1) + fs;
	str += to_string(elapsed_seconds.count()) + fs;
	str += to_string(total_time) + fs;
	str += to_string(total_size) + fs;
	str += to_string(count_fetching_error) + fs;
	str += to_string(s.get_numDomainsQueue(BUSY)) + fs;
	str += to_string(s.get_numDomainsQueue(NEW)) + fs;
	str += to_string(s.get_numDomainsQueue(IDLE));
	report_output << str + "\n";
}

void *crawler_thread(void *data) {
	int id = (long) data;
	vector<string> write_buffer;
	ofstream output_file("data/saida_thread"+to_string(id)+".txt");
	bool success;
	int curr_level, curr_domain_id;
	size_t size;
	vector<n_url> urls_to_add;
	pair<string, int> v;
	string url, new_url, new_url_key, domain_str;

	chrono::time_point<std::chrono::system_clock> start, end;

	while(1) {

		CkSpider spider;
		spider.put_Utf8(true);
		spider.put_ConnectTimeout(4);
		spider.put_ReadTimeout(16);
		spider.put_MaxResponseSize(2000000);
		spider.put_MaxUrlLen(150);

		if (count_fetched >= LIMIT_URLS) {
			break;
		}

		if (s.get_numAvailable() == 0) {
			spider.SleepMs(300);
			continue;
		}

		v = s.get_url();
		url = v.first;
		curr_domain_id = v.second;

		if (url == "\0") {
			spider.SleepMs(300);
			continue;
		}
		spider.Initialize(url.c_str());
	
		start = chrono::system_clock::now(); 
		success = spider.CrawlNext();
		end = chrono::system_clock::now(); 

		// after crawl, we can release the domain
		// s.release_domain(curr_domain_id);

		chrono::duration<double> elapsed_seconds = end - start; 

		if (success == false) {
			cout << url << "\t erro ao fazer fetch da url\n";
			s.put_stats(curr_domain_id, 0, 0, true);
			pthread_mutex_lock(&mutex);
			count_fetching_error++;
			pthread_mutex_unlock(&mutex);
			continue; 
		}
		// 
		domain_str = spider.getUrlDomain(url.c_str());
		if (domain_str.find("www.") == 0) {
			domain_str = domain_str.substr(4);
		}

		size = strlen(spider.lastHtml());
		curr_level = s.get_domainLevel(spider.domain());

		s.put_stats(curr_domain_id, size, elapsed_seconds.count(), false);

		pthread_mutex_lock(&mutex);
		total_size += size;
		total_time += elapsed_seconds.count();
		count_fetched++;
		if (curr_level == 1) {
			count_fetched_lv1++;
		}
		if (!(count_fetched % 100)) {
			cout << "fetched: " << count_fetched << "\n"; 
		}
		if (!(count_fetched % (LIMIT_URLS/10))) {
			report();
		}
		pthread_mutex_unlock(&mutex);
		
		write_buffer.push_back(
			get_rowString(
				spider.lastUrl(), 
				spider.lastHtmlTitle(), 
				spider.lastHtml()
			)
		);

		if (write_buffer.size() > MAX_WRITE_BUFFER_SZ) {
			for (string row : write_buffer) {
				output_file << row;
			}
			write_buffer.clear();
		}
		
		urls_to_add.clear();

		for (int i = 0; i < spider.get_NumOutboundLinks(); i++) {
			new_url = spider.getOutboundLink(i);
			auto new_domain = spider.getUrlDomain(new_url.c_str());
			if (new_domain != NULL) {
				new_url_key = s.get_urlKey(new_url);
				urls_to_add.push_back({
					new_url, 
					new_url_key, 
					string(new_domain), 
					curr_level,
					curr_domain_id
				});
			}
		}

		for (int i = 0; i < spider.get_NumUnspidered(); i++) {
			new_url = spider.getUnspideredUrl(i);
			auto new_domain = spider.getUrlDomain(new_url.c_str());
			new_url_key = s.get_urlKey(new_url);
			urls_to_add.push_back({
				new_url, 
				new_url_key, 
				string(new_domain), 
				curr_level,
				curr_domain_id
			});
		}

		s.put_urlList(urls_to_add);
	}

	for (string row : write_buffer) {
		output_file << row;
	}

	output_file.close();
	pthread_exit(EXIT_SUCCESS);
}

void *release_domains(void *data) {
	int len;
	CkSpider spider;

	while (1) {	
		len = s.get_numDomainsQueue(BUSY);
		spider.SleepMs(WAITING_TIME_MS);
		if (len > 0) {
			s.release_busy(len);
		}
		spider.SleepMs(WAITING_TIME_MS);
	}
}

int main(int argc, char *argv[]) {
	CkSpider spider;
	string url;
	pthread_t tid[NUM_THREADS];
	pthread_t t_release;
	void *data;
	int count_seeds = 0;

	s = Scheduler();
	pthread_mutex_init(&mutex, NULL);
	count_fetched = 0;

	count_fetching_error = 0;
	total_time = 0;
	total_size = 0;
	count_fetched_lv1 = 0;
	report_output = ofstream("data/reports.csv");
	g_start = chrono::system_clock::now(); 

	// setting header for csv 
	string fs = FIELD_SEPARATOR;
	report_output << "total domains"+fs+"total fetched"+fs;
	report_output << "total fetched lv 1"+fs+"total elapsed time"+fs;
	report_output << "total fetching time in sec"+fs+"total size in bytes"+fs;
	report_output << "total of fetching errors"+fs;
	report_output << "num of busy domains"+fs+"num of new domains"+fs+"num of idle domains";
	report_output << "\n";

	while (getline(cin, url)) {
		s.put_seed(url);
		count_seeds++;
	}

	pthread_create(&t_release, NULL, release_domains, NULL);

	for (long i = 0; i < NUM_THREADS; i++) {
		pthread_create(&tid[i], NULL, crawler_thread, (void *) i);
		spider.SleepMs(100);
	}

	for (int i = 0; i < NUM_THREADS; i++) {
		pthread_join(tid[i], &data);
	}
	pthread_cancel(t_release);
	pthread_mutex_destroy(&mutex);

	auto t = s.get_stats();
	// for (auto itr : t) {
	// 	cout << "domain:\t" << itr.domain << "\n";
	// 	cout << "size:\t" << itr.size << "\n";
	// 	cout << "count_fetched:\t" << itr.count_fetched << "\n";
	// 	cout << "count_errors:\t" << itr.count_errors << "\n";
	// 	cout << "count_total:\t" << itr.count_total << "\n";
	// 	cout << "num_childs:\t" << itr.num_childs << "\n";
	// 	cout << "time_elapsed:\t" << itr.time_elapsed << "\n\n\n";
	// }

	ofstream output("data/report_final.csv");
	
	output << "domain"+fs+"size"+fs+"count_fetched"+fs+"count_errors"+fs+"count_total"+fs+"num_childs"+fs+"time_elapsed\n";
	for (auto itr : t) {
		output << itr.domain << fs;
		output << itr.size << fs;
		output << itr.count_fetched << fs;
		output << itr.count_errors << fs;
		output << itr.count_total << fs;
		output << itr.num_childs << fs;
		output << itr.time_elapsed << "\n";
	}
	
	output.close();

	return 0;
}
