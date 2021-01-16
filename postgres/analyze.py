import matplotlib.pyplot as plt
from collections import defaultdict

def show_plot_for(data, smallest, title):
    x = []
    y = []
    for el in data:
        x.append(el[0])
        y.append(el[1])
    
    s_y = sorted(y)
    print(f'SLA for {title}')
    print('95%%: %.2f ms, 99%%: %.2f ms, 99.9%%: %.2f ms' %
          (s_y[int((len(s_y) - 1) * 0.95)],
           s_y[int((len(s_y) - 1) * 0.99)],
           s_y[int((len(s_y) - 1) * 0.999)]))
    plt.plot(x, y, 'o', color='black', markersize=0.7);
    plt.title(title + f': {len(data)} requests')
    plt.show()

def split_data(results):
    ret = defaultdict(list)
    for el in results:
        ret[0 if el[2] == 0 else 1].append(el[0:2])
    return ret

def show_results(results, all_number):
    print('Preparing results')
    print(f'Handled {len(results)} of {all_number} results ({round(100 * len(results) / all_number, 2)}%)')
    
    smallest = results[0][0]
    results = split_data(results)
    show_plot_for(results[0], smallest, f"Immediately send ad")
    show_plot_for(results[1], smallest, f"Send ad after processing")
    
    
    

