#include <CkSpider.h>

#include <queue>
#include <vector>
#include <unordered_map>
#include <string>
#include <pthread.h>

#include <iostream>

#define MAX_WIDTH 10000	// numero maximo de dominios distintos 
#define MAX_DEPTH 1

#define NEW 0
#define IDLE 1
#define BUSY 2

using namespace std;

typedef struct domain_info {
	string domain;
	int id;
	int level;
	bool has_started;
    int parent_id;
} d_info;

typedef struct url_info {
    string url;
    int weight;
    bool operator <(const url_info &u) const {
		return weight > u.weight;
	}
} url_t;

typedef struct new_url_data {
	string url;
    string url_key;
	string domain;
	int curr_level;
	int parent_id;
} n_url;

typedef struct domain_statistics {
    string domain;
    size_t size;
    int count_fetched;
    int count_errors;
    int count_total;
    int num_childs;
    double time_elapsed;
} d_stats;

class Scheduler {
    
    private:
        pthread_mutex_t mutex;
        int count_domains;
        int num_available;
        unordered_map<string, d_info> domains;
        unordered_map<string, bool> urls;
        queue<int> busy_q, idle_q, new_q;
        priority_queue<url_t> urls_queue[MAX_WIDTH];

        d_stats domains_stats[MAX_WIDTH]; 
    
        int get_urlWeight(string url);
        int get_urlSize(string url);
        void put_url(n_url u);

    public:
        Scheduler();
        ~Scheduler();
        int get_numAvailable();
        int get_numDomains();
        int get_numDomainsQueue(int queue_id);
        int get_domainId(string domain_str);
        int get_domainLevel(string domain_str);
        string get_urlKey(string url);
        pair<string, int> get_url();
        void put_seed(string url);
        void put_urlList(vector<n_url> list);
        void release_busy(int num);
        void release_domain(int d_id);

        vector<d_stats> get_stats();
        void put_stats(int d_id, int size, double time_elapsed, bool error);

};


