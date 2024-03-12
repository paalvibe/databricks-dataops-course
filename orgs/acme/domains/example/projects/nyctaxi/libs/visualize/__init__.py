import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def plot_distributions(df, cols, add_mean=False):
  row_count = df.count()
  target_size = 1000000
  
  num_df = df.select(cols)
  
  # If the input dataframe is super big, sample the rows to a more manageable size.
  if row_count > 2 * target_size:
    fraction = target_size / row_count
    sampled_df = num_df.sample(False, fraction)
  else:
    sampled_df = num_df
  
  sampled_df.cache()
  
  plot_count = len(cols)
  counter = 1
  
  fig = plt.figure()

  for col in cols:
    col_name = cols[counter - 1]
    values = np.array(sampled_df.select(col_name).collect())

    # Let Seaborn plot the distribution
    ax = plt.subplot(plot_count, 1, counter)
    ax = sns.distplot(values)

    if add_mean:
        # Add vertical lines showing the mean and +/- 1 standard deviation
        mean = np.mean(values)
        sigma1, sigma2 = mean - np.std(values), mean + np.std(values)
        min, max = np.min(values), np.max(values)
        padding = (max - min) * 0.005

        ax.axvline(mean, color='#c00000', linestyle='dashed', linewidth=1)
        ax.annotate("$\mu$={:,.1f}".format(mean), xy=(mean + padding, 0), ha='left', fontsize=10)

        ax.axvline(sigma1, color='#202020', linestyle='dashed', linewidth=0.5, alpha=0.37)
        ax.axvline(sigma2, color='#202020', linestyle='dashed', linewidth=0.5, alpha=0.37)

    ax.set_title("Distribution  of  \"{}\"".format(col_name), {'fontsize': '20'})
    ax.set_ylabel('probability')
    
    counter += 1

  fig.suptitle("Distribution of Numeric Values", fontsize=35)
  fig.set_dpi(96)
  fig.set_size_inches(18, 6.5 * plot_count)
  
  if plot_count == 1:
    plt.subplots_adjust(top = 0.8)
  else:
    plt.subplots_adjust(hspace=0.5)

  return(fig)