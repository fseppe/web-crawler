#include <CkSpider.h>

#include <algorithm>
#include <vector>
#include <unordered_map>
#include <queue>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>
#include <pthread.h>

#define MAX_WIDTH 100000	// numero maximo de dominios distintos 
#define MAX_DEPTH 1
#define LIMIT_URLS 1001
#define NUM_THREADS 40
#define WAITING_TIME_MS 100
#define FIELD_SEPARATOR "|;|;|"

using namespace std;
// using namespace std::chrono;

static pthread_mutex_t mutex;
static pthread_cond_t mutex_cond;

typedef struct domain_info {
	string domain;
	int id;
	int level;
	bool has_started;
} d_info;

typedef struct new_url_data {
	string url;
	string domain;
	int curr_level;
} n_url;

unordered_map<string, d_info> domains;
unordered_map<string, bool> urls;
queue<int> busy_q, idle_q, new_q;
queue<string> urls_queue[MAX_WIDTH];
int count_domains;
int num_available;
int count_fetched;
ofstream output_file;

/* 
	verrificar dominios "distinto". ex:
		https://uol.com.br/tilt/negocios/
		https://www.uol.com.br/tilt/review/
*/

/*
	limitar tamanho maximo da url para 100 (?) 
*/

/*
	criar buffer para escrita do documento
*/

void write_document(string url, string title, string html) {
	string fs = string(FIELD_SEPARATOR);
	replace(url.begin(), url.end(), '\n', ' ');
	replace(url.begin(), url.end(), '\r', ' ');
	replace(title.begin(), title.end(), '\n', ' ');
	replace(title.begin(), title.end(), '\r', ' ');
	replace(html.begin(), html.end(), '\n', ' ');
	replace(html.begin(), html.end(), '\r', ' ');
	output_file << url + fs + title << "\n";
	// output_file << url + fs + title + fs + html << "\n";
}

void put_seed(string url) {
	CkSpider spider;
	string domain;
	int d_id;
	
	d_id = count_domains;
	domain = spider.getUrlDomain(url.c_str());

	domains[domain] = {domain, d_id, 0, true};

	new_q.push(d_id);
	urls_queue[d_id].push(url);

	urls[url] = true;

	count_domains++;
	num_available++;
}

void put_url(string url, string domain_str, int level) {
	d_info domain = domains[domain_str];
	int new_id;

	if (!domain.has_started && count_domains >= MAX_WIDTH) {
		return;
	}

	/* 
		caso seja um dominio conhecido, ha duas opcoes:
			1. esta em uma das filas e nao precisar ser feito nada de especial
			2. a lista de urls do dominio esta vazia e entao eh preciso adicionar a lista de ocupados

		OBS: eh adicionado a lista de ocupados para evitar que entre imediatamente no dominio (?)
	*/
	if (domain.has_started && domain.level <= MAX_DEPTH) {
		new_id = domain.id;
		if (urls_queue[new_id].size() == 0) {
			busy_q.push(new_id);
		}
		if (busy_q.size() == 1) {
			pthread_cond_signal(&mutex_cond);
		}
	}

	else if (!domain.has_started && level < MAX_DEPTH) {
		new_id = count_domains;
		new_q.push(new_id);
		domains[domain_str] = {
			domain_str, 
			new_id, 
			level+1, 
			true
		};
		count_domains++;
	}

	else {
		return;
	}
	
	urls_queue[new_id].push(url);
	urls[url] = true;
	num_available++;
}

string get_url() {
	string url;
	int id;

	if (!new_q.empty()) {
		// pega o id do dominio e remove o id da lista de novos
		id = new_q.front(); 
		new_q.pop();
		
		// pega a url e remove da lista de urls desse dominio
		url = urls_queue[id].front();
		urls_queue[id].pop();

		// se o dominio nao tiver ficado vazio, adiciona ele a lista de dominios ocupados e caso a lista de ocupados estivesse vazia, sinaliza para que o laco de liberar dominios ocupados volte a funcionar
		if (!urls_queue[id].empty()) {
			busy_q.push(id);
			if (busy_q.size() == 1) 
				pthread_cond_signal(&mutex_cond); 
		}
	}
	
	else if (!idle_q.empty()) {
		id = idle_q.front(); 
		idle_q.pop();

		url = urls_queue[id].front();
		urls_queue[id].pop();
	
		if (!urls_queue[id].empty()) {
			busy_q.push(id);
			if (busy_q.size() == 1) 
				pthread_cond_signal(&mutex_cond);
		}
	}
	
	else {
		return "\0";
	}

	num_available--;
	return url;
}

void *crawler_thread(void *data) {
	// int id = (long) data;
	int curr_level;
	vector<n_url> urls_to_add;
	CkSpider spider;
	string url, new_url;

	while(1) {
		if (count_fetched >= LIMIT_URLS) {
			break;
		}

		if (num_available == 0) {
			spider.SleepMs(1000);
			continue;
		}

		pthread_mutex_lock(&mutex);
		url = get_url();
		pthread_mutex_unlock(&mutex);
		
		if (url == "\0") {
			spider.SleepMs(200);
			continue;
		}
	
		spider.Initialize(url.c_str());
		spider.CrawlNext();

		curr_level = domains[spider.domain()].level;

		if (!(count_fetched % 100))
			cout << "fetched: " << count_fetched << "\n"; 
		
		write_document(spider.lastUrl(), spider.lastHtmlTitle(), spider.lastHtml());
		cout << spider.lastUrl() << "\n";
		
		urls_to_add.clear();

		for (int i = 0; i < spider.get_NumOutboundLinks(); i++) {
			new_url = spider.getOutboundLink(i);
			auto new_domain = spider.getUrlDomain(new_url.c_str());
			if (new_domain != NULL) {
				if (!urls[new_url]) {
					urls_to_add.push_back({new_url, string(new_domain), curr_level});
				}
			}
			else {
				cout << "[log] null domain for: " << new_url << "\n";
			}
		}

		for (int i = 0; i < spider.get_NumUnspidered(); i++) {
			new_url = spider.getUnspideredUrl(i);
			if (!urls[new_url]) {
				urls_to_add.push_back({new_url, string(spider.domain()), curr_level});
			}
		}

		pthread_mutex_lock(&mutex);
		count_fetched++;
		for (n_url itr : urls_to_add) {
			put_url(itr.url, itr.domain, itr.curr_level);
		}
		pthread_mutex_unlock(&mutex);
	}

	pthread_exit(EXIT_SUCCESS);
}

void *release_domains(void *data) {
	int len, d_id;
	CkSpider spider;

	while (1) {
		pthread_mutex_lock(&mutex);
		while (busy_q.size() < 1) {
			pthread_cond_wait(&mutex_cond, &mutex);
		}
		len = busy_q.size();
		
		// verificando desempenho com sleep dentro do trava
		spider.SleepMs(WAITING_TIME_MS);

		while (!busy_q.empty() && len > 0) {
			d_id = busy_q.front();
			busy_q.pop();
			idle_q.push(d_id);
			len--;
		}
		pthread_mutex_unlock(&mutex);
	}
}

void *reports(void *data) {
	// check the size of queue before waiting and then pop until be empty or reached the size stored
	CkSpider spider;
	string file_name;
	int total_urls, total_domains, queue_domains, not_empty_domains;
	int domains_busy, domains_new, domains_idle;
	int c = 0;
	ofstream report_output = ofstream("data/relatorios.txt");
	while (1) {
		total_urls = total_domains = queue_domains = not_empty_domains = 0;
		spider.SleepMs(5000);

		pthread_mutex_lock(&mutex);
		for (auto itr : domains) {
			if(!itr.second.has_started) continue;
			total_urls += urls_queue[itr.second.id].size();
			if (!urls_queue[itr.second.id].empty()) {
				not_empty_domains++;
			}
			total_domains++;
		}
		domains_busy = busy_q.size();
		domains_idle = idle_q.size();
		domains_new = new_q.size();

		pthread_mutex_unlock(&mutex);

		queue_domains += domains_busy + domains_idle + domains_idle;

		report_output << "atual: " << c;
		report_output << " - domains_busy: " << domains_busy;
		report_output << " - domains_idle: " << domains_idle;
		report_output << " - domains_new: " << domains_new;
		report_output << " - total_urls: " << total_urls;
		report_output << " - total_domains: " << total_domains;
		report_output << " - not_empty_domains: " << not_empty_domains;
		report_output << " - queue_domains: " << queue_domains;
		report_output << "\n";

		c++;
	}

	report_output.close();
}

void final_report() {

	ofstream report_output = ofstream("data/relatorio_final.txt");
	int total_urls, total_domains, queue_domains;
	total_urls = total_domains = queue_domains = 0;

	for (auto itr : domains) {
		if(!itr.second.has_started) continue;
		report_output << "Domain: " << itr.second.domain;
		report_output << " - first: " << itr.first;
		report_output << " - ID: " << itr.second.id;
		report_output << " - Level: " << itr.second.level;
		report_output << " - has_started: " << itr.second.has_started;
		report_output << " - urls size: " << urls_queue[itr.second.id].size();
		report_output << " - urls: ";

		total_urls += urls_queue[itr.second.id].size();
		if (!urls_queue[itr.second.id].empty()) {
			total_domains++;
		}

		while (!urls_queue[itr.second.id].empty()) {
			report_output << urls_queue[itr.second.id].front() << " ";
			urls_queue[itr.second.id].pop();
		}
		report_output << "\n";
	}
	report_output << "\n\n\n";
	report_output << "busy_q: ";
	while(!busy_q.empty()) {
		queue_domains++;
		report_output << busy_q.front() << " ";
		busy_q.pop();
	}
	report_output << "\n";
	report_output << "idle_q: ";
	while(!idle_q.empty()) {
		queue_domains++;
		report_output << idle_q.front() << " ";
		idle_q.pop();
	}
	report_output << "\n";
	report_output << "new_q: ";
	while(!new_q.empty()) {
		queue_domains++;
		report_output << new_q.front() << " ";
		new_q.pop();
	}
	report_output << "\n\n\n";

	report_output << "total_urls: " << total_urls;
	report_output << " - total_domains: " << total_domains;
	report_output << " - queue_domains: " << queue_domains;
	report_output << "\n";

	report_output.close();
}

int main(int argc, char *argv[]) {
	CkSpider spider;
	string url;
	pthread_t tid[NUM_THREADS];
	pthread_t t_release, t_reports;
	void *data;
	int count_seeds = 0;

	pthread_mutex_init(&mutex, NULL);
	pthread_cond_init(&mutex_cond, NULL);

	output_file = ofstream("data/saida.txt");

	count_domains = 0;
	count_fetched = 0;
	num_available = 0;

	while (getline(cin, url)) {
		put_seed(url);
		count_seeds++;
	}

	pthread_create(&t_release, NULL, release_domains, NULL);
	pthread_create(&t_reports, NULL, reports, NULL);

	for (long i = 0; i < NUM_THREADS; i++) {
		pthread_create(&tid[i], NULL, crawler_thread, (void *) i);
		spider.SleepMs(100);
	}

	for (int i = 0; i < NUM_THREADS; i++) {
		pthread_join(tid[i], &data);
	}

	pthread_cancel(t_release);
	pthread_cancel(t_reports);

	final_report();
	
	output_file.close();

	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&mutex_cond);

	return 0;
}