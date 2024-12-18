import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def make_n_variance_graph(path):
    data = pd.read_csv(path, sep='\t', index_col=False)
    get_means = []
    put_means = []
    get_medians = []
    put_medians = []
    get_p90s = []
    put_p90s = []
    for n_value in range(10, 101, 5):
        gets = data.loc[(data['action'] == 'GET') & (data['n'] == n_value)]
        puts = data.loc[(data['action'] == 'PUT') & (data['n'] == n_value)]
        get_vals = gets['time_micros'].values / 1000 # Converting to ms
        put_vals = puts['time_micros'].values / 1000 # Converting to ms

        get_means.append(np.mean(get_vals))
        put_means.append(np.mean(put_vals))
        get_medians.append(np.percentile(get_vals, 50))
        put_medians.append(np.percentile(put_vals, 50))
        get_p90s.append(np.percentile(get_vals, 90))
        put_p90s.append(np.percentile(put_vals, 90))
    
    plt.figure()
    plt.title('Mean Request Latency: c=50, k=3')
    plt.xlabel('N value (number of storage nodes)')
    plt.ylabel('Mean Request Latency (ms)')
    plt.plot(range(10, 101, 5), get_means, label='GET Requests')
    plt.plot(range(10, 101, 5), put_means, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()

    plt.figure()
    plt.title('Median Request Latency: c=50, k=3')
    plt.xlabel('N value (number of storage nodes)')
    plt.ylabel('Median Request Latency (ms)')
    plt.plot(range(10, 101, 5), get_medians, label='GET Requests')
    plt.plot(range(10, 101, 5), put_medians, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()

    plt.figure()
    plt.title('90th Percentile Request Latency: c=50, k=3')
    plt.xlabel('N value (number of storage nodes)')
    plt.ylabel('90th Percentile Request Latency (ms)')
    plt.plot(range(10, 101, 5), get_p90s, label='GET Requests')
    plt.plot(range(10, 101, 5), put_p90s, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()
    
    print('Experiment-wide statistics below:')
    get_vals = data.loc[data['action'] == 'GET']['time_micros'].values / 1000 # Converting to ms
    put_vals = data.loc[data['action'] == 'PUT']['time_micros'].values / 1000

    print('Mean response time - GET: {}ms, PUT: {}ms'.format(np.mean(get_vals), np.mean(put_vals)))
    print('Median response time - GET :{}ms, PUT: {}ms'.format(np.percentile(get_vals, 50), np.percentile(put_vals, 50)))
    print('90th percentile response time - GET :{}ms, PUT: {}ms'.format(np.percentile(get_vals, 90), np.percentile(put_vals, 90)))


def make_k_variance_graph(path):
    data = pd.read_csv(path, sep='\t', index_col=False)
    get_means = []
    put_means = []
    get_medians = []
    put_medians = []
    get_p90s = []
    put_p90s = []
    for k_value in range(1, 5):
        gets = data.loc[(data['action'] == 'GET') & (data['k'] == k_value)]
        puts = data.loc[(data['action'] == 'PUT') & (data['k'] == k_value)]
        get_vals = gets['time_micros'].values / 1000 # Converting to ms
        put_vals = puts['time_micros'].values / 1000 # Converting to ms

        get_means.append(np.mean(get_vals))
        put_means.append(np.mean(put_vals))
        get_medians.append(np.percentile(get_vals, 50))
        put_medians.append(np.percentile(put_vals, 50))
        get_p90s.append(np.percentile(get_vals, 90))
        put_p90s.append(np.percentile(put_vals, 90))
    
    plt.figure()
    plt.title('Mean Request Latency: n=20, c=50')
    plt.xlabel('K value (replication factor)')
    plt.ylabel('Mean Request Latency (ms)')
    plt.plot(range(1, 5), get_means, label='GET Requests')
    plt.plot(range(1, 5), put_means, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()

    plt.figure()
    plt.title('Median Request Latency: n=20, c=50')
    plt.xlabel('K value (replication factor)')
    plt.ylabel('Median Request Latency (ms)')
    plt.plot(range(1,5), get_medians, label='GET Requests')
    plt.plot(range(1,5), put_medians, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()

    plt.figure()
    plt.title('90th Percentile Request Latency: n=20, c=50')
    plt.xlabel('N value (number of storage nodes)')
    plt.ylabel('90th Percentile Request Latency (ms)')
    plt.plot(range(1,5), get_p90s, label='GET Requests')
    plt.plot(range(1,5), put_p90s, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()
    
    print('Experiment-wide statistics below:')
    get_vals = data.loc[data['action'] == 'GET']['time_micros'].values / 1000 # Converting to ms
    put_vals = data.loc[data['action'] == 'PUT']['time_micros'].values / 1000

    print('Mean response time - GET: {}ms, PUT: {}ms'.format(np.mean(get_vals), np.mean(put_vals)))
    print('Median response time - GET :{}ms, PUT: {}ms'.format(np.percentile(get_vals, 50), np.percentile(put_vals, 50)))
    print('90th percentile response time - GET :{}ms, PUT: {}ms'.format(np.percentile(get_vals, 90), np.percentile(put_vals, 90)))

def make_c_variance_graph(path):
    data = pd.read_csv(path, sep='\t', index_col=False)
    get_means = []
    put_means = []
    get_medians = []
    put_medians = []
    get_p90s = []
    put_p90s = []
    for c_value in range(10, 101, 5):
        gets = data.loc[(data['action'] == 'GET') & (data['c'] == c_value)]
        puts = data.loc[(data['action'] == 'PUT') & (data['c'] == c_value)]
        get_vals = gets['time_micros'].values / 1000 # Converting to ms
        put_vals = puts['time_micros'].values / 1000 # Converting to ms

        get_means.append(np.mean(get_vals))
        put_means.append(np.mean(put_vals))
        get_medians.append(np.percentile(get_vals, 50))
        put_medians.append(np.percentile(put_vals, 50))
        get_p90s.append(np.percentile(get_vals, 90))
        put_p90s.append(np.percentile(put_vals, 90))
    
    plt.figure()
    plt.title('Mean Request Latency: n=20, k=3')
    plt.xlabel('C value (number of concurrent clients)')
    plt.ylabel('Mean Request Latency (ms)')
    plt.plot(range(10, 101, 5), get_means, label='GET Requests')
    plt.plot(range(10, 101, 5), put_means, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()

    plt.figure()
    plt.title('Median Request Latency: n=20, k=3')
    plt.xlabel('C value (number of concurrent clients)')
    plt.ylabel('Median Request Latency (ms)')
    plt.plot(range(10, 101, 5), get_medians, label='GET Requests')
    plt.plot(range(10, 101, 5), put_medians, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()

    plt.figure()
    plt.title('90th Percentile Request Latency: n=20, k=3')
    plt.xlabel('C value (number of concurrent clients)')
    plt.ylabel('90th Percentile Request Latency (ms)')
    plt.plot(range(10, 101, 5), get_p90s, label='GET Requests')
    plt.plot(range(10, 101, 5), put_p90s, label='PUT Requests')
    plt.legend(loc='upper left')
    plt.show()
    
    print('Experiment-wide statistics below:')
    get_vals = data.loc[data['action'] == 'GET']['time_micros'].values / 1000 # Converting to ms
    put_vals = data.loc[data['action'] == 'PUT']['time_micros'].values / 1000

    print('Mean response time - GET: {}ms, PUT: {}ms'.format(np.mean(get_vals), np.mean(put_vals)))
    print('Median response time - GET :{}ms, PUT: {}ms'.format(np.percentile(get_vals, 50), np.percentile(put_vals, 50)))
    print('90th percentile response time - GET :{}ms, PUT: {}ms'.format(np.percentile(get_vals, 90), np.percentile(put_vals, 90)))


if __name__ == '__main__':
    make_n_variance_graph('cleaner_n_variance.tsv')
    make_k_variance_graph('cleaner_k_variance.tsv')
    make_c_variance_graph('cleaner_c_variance.tsv')
