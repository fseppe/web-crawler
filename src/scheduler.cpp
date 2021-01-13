#include "scheduler.h"

Scheduler::Scheduler() {
    this->mutex = PTHREAD_MUTEX_INITIALIZER;
    this->count_domains = 0;
    this->num_available = 0;
	for (int i = 0; i < MAX_WIDTH; i++) {
		this->domains_stats[i] = {
			"\0",	// domain
			0,		// size
			0,		// count_fetched
			0,		// count_errors
			0,		// count_total
			0,		// num_childs
			0		// time_elapsed
		};
	}
}

Scheduler::~Scheduler() {
    pthread_mutex_destroy(&this->mutex);
}

int Scheduler::get_urlWeight(string url) {
	// if it indicates that the url is not br, add 10 to the weight
    int is_br = (
		url.find("br/") == string::npos &&
		url.find("/br") == string::npos &&
		url.find(".br") == string::npos && 
		url.find("br.") == string::npos 
	) ? 10 : 0;
	return is_br + this->get_urlSize(url);	
}

int Scheduler::get_urlSize(string url) {
	int size = 0;
	for (size_t i = url.find("://")+3; i < url.size(); i++) {
		if (url[i] == '.' or url[i] == '/') {
			size++;
		}
	}
	return size;
}

void Scheduler::put_url(n_url u) {

    this->urls[u.url_key] = true;

	if (
		u.url.substr(u.url.size()-5, u.url.size()) == string(".jpeg") || 
		u.url.substr(u.url.size()-4, u.url.size()) == string(".jpg") || 
		u.url.substr(u.url.size()-4, u.url.size()) == string(".bmp") ||
		u.url.substr(u.url.size()-4, u.url.size()) == string(".gif") || 
		u.url.substr(u.url.size()-4, u.url.size()) == string(".pdf") || 
		u.url.substr(u.url.size()-4, u.url.size()) == string(".mp4") || 
		u.url.substr(u.url.size()-4, u.url.size()) == string(".avi") || 
		u.url.substr(u.url.size()-4, u.url.size()) == string(".mp3") 
	) {
		return;
	}

	// if starts with www.
	if (u.domain.find("www.") == 0) {
		u.domain = u.domain.substr(4);
	}

    int new_id;
	CkSpider spider;
    url_t new_url;
	d_info curr_domain = this->domains[u.domain];

	if (!curr_domain.has_started && this->count_domains >= MAX_WIDTH) {
		return;
	}
        
    if (curr_domain.has_started && curr_domain.level <= MAX_DEPTH) {
		new_id = curr_domain.id;
		if (this->urls_queue[new_id].empty()) {
			this->busy_q.push(new_id);
		}
	}

    else if (!curr_domain.has_started && u.curr_level < MAX_DEPTH) {
		new_id = this->count_domains;
		this->new_q.push(new_id);
		
		// creating new domain
		this->domains[u.domain] = {
			u.domain, 
			new_id, 
			u.curr_level+1, 
			true,
			u.parent_id
		};
		this->count_domains++;

		// stats
		this->domains_stats[u.parent_id].num_childs++;
		this->domains_stats[new_id].domain = u.domain;
	}

    else {
        return;
    }

    this->urls_queue[new_id].push({u.url, this->get_urlWeight(u.url)});
	this->num_available++;

	// stats
	this->domains_stats[new_id].count_total++;

}

int Scheduler::get_numAvailable() {
    return this->num_available;
}

int Scheduler::get_numDomains() {
    return this->count_domains;
}

int Scheduler::get_numDomainsQueue(int queue_id) {
    if (queue_id == NEW) {
        return this->new_q.size();
    }
    if (queue_id == IDLE) {
        return this->idle_q.size();
    }
    if (queue_id == BUSY) {
        return this->busy_q.size();
    }
    return -1;
}

int Scheduler::get_domainId(string domain_str) {
	if (domain_str.find("www.") == 0) {
		domain_str = domain_str.substr(4);
	}
    return this->domains.at(domain_str).id;
}

int Scheduler::get_domainLevel(string domain_str) {
	if (domain_str.find("www.") == 0) {
		domain_str = domain_str.substr(4);
	}
    return this->domains.at(domain_str).level;
}

string Scheduler::get_urlKey(string url) {
	size_t pos;

	pos = url.find("://");
	if (pos == string::npos) {
		return "\0";
	}
	
	url = url.substr(pos+3);

	pos = 0;
	while ((pos = url.find("//", pos)) != string::npos) {
		url.replace(pos, 2, "/");
	}

	if (url[url.size()-1] == '/') {
		url.resize(url.size()-1);
	}

	return url;
}

pair<string, int> Scheduler::get_url() {

	string url;
	int id;

    pthread_mutex_lock(&this->mutex);
	if (!this->new_q.empty()) {
		// pega o id do dominio e remove o id da lista de novos
		id = this->new_q.front(); 
		this->new_q.pop();
		
		// pega a url e remove da lista de urls desse dominio
		url = this->urls_queue[id].top().url;
		this->urls_queue[id].pop();

		if (!this->urls_queue[id].empty()) {
			this->busy_q.push(id);
		}
	}
	
	else if (!this->idle_q.empty()) {
		id = this->idle_q.front(); 
		this->idle_q.pop();
		url = this->urls_queue[id].top().url;
		this->urls_queue[id].pop();
		if (!this->urls_queue[id].empty()) {
			this->busy_q.push(id);
		}
	}
	
	else {
		id = 0;
		url = "\0";
	}

	if (url != "\0") {
        this->num_available--;
    }
    pthread_mutex_unlock(&this->mutex);

	return make_pair(url, id);
}

void Scheduler::put_seed(string url) {
    CkSpider spider;

	int d_id = this->count_domains;
	string domain = spider.getUrlDomain(url.c_str());

    // if starts with www.
	if (domain.find("www.") == 0) {
		domain = domain.substr(4);
	}

	this->domains[domain] = {domain, d_id, 0, true, -1};

	this->new_q.push(d_id);
	this->urls_queue[d_id].push({url, 0});

	this->urls[this->get_urlKey(url)] = true;

	this->count_domains++;
	this->num_available++;

	this->domains_stats[d_id].count_total++;
	this->domains_stats[d_id].domain = domain;
}

void Scheduler::put_urlList(vector<n_url> list) {
    pthread_mutex_lock(&this->mutex);
    for (n_url itr : list) {
        if (!this->urls[itr.url_key]) {
            this->put_url(itr);
        }
    }
    pthread_mutex_unlock(&this->mutex);
}

void Scheduler::release_busy(int num) {
    int d_id;
    pthread_mutex_lock(&this->mutex);
    while (!this->busy_q.empty() && num > 0) {
        d_id = this->busy_q.front();
        this->busy_q.pop();
        this->idle_q.push(d_id);
        num--;
    }
    pthread_mutex_unlock(&this->mutex);
}

void Scheduler::release_domain(int id) {
	// se o dominio nao tiver ficado vazio, adiciona ele a lista de dominios ocupados e caso a lista de ocupados estivesse vazia, sinaliza para que o laco de liberar dominios ocupados volte a funcionar
	pthread_mutex_lock(&this->mutex);
	if (!this->urls_queue[id].empty()) {
		this->busy_q.push(id);
	}
	pthread_mutex_unlock(&this->mutex);
}

vector<d_stats> Scheduler::get_stats() {
	vector<d_stats> to_return;
	pthread_mutex_lock(&this->mutex);
	for (int i = 0; i < MAX_WIDTH; i++) {
		if (this->domains_stats[i].count_total > 0)
			to_return.push_back(this->domains_stats[i]);
	}
	pthread_mutex_unlock(&this->mutex);
	return to_return;
}

void Scheduler::put_stats(int d_id, int size, double time_elapsed, bool error) {
	pthread_mutex_lock(&this->mutex);
	if (error) {
		this->domains_stats[d_id].count_errors++;
	}
	else {
		this->domains_stats[d_id].size += size;
		this->domains_stats[d_id].time_elapsed += time_elapsed;
		this->domains_stats[d_id].count_fetched++;
	}
	pthread_mutex_unlock(&this->mutex);
}

