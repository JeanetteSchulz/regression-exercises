import seaborn as sns
import matplotlib.pyplot as plt

def plot_variable_pairs(df):
    sns.lmplot(data=df, x='monthly_charges', y='total_charges', line_kws={'color':'red'})
    sns.lmplot(data=df, x='tenure', y='monthly_charges', line_kws={'color':'red'})
    sns.lmplot(data=df, x='tenure', y='total_charges', line_kws={'color':'red'})
    plt.show()
    return df.corr()['has_churned']

def plot_categorical_and_continuous_vars(df, categorical, continuous):
    # Loop through the list of continuous variables
    for con in continuous:
        # Loop through categorical list, comparing to the current categorical
        for cat in categorical:
            # Plot three plots of the current continuous and categorical
            plt.figure(figsize = (20,10))
            plt.subplot(1,3,1)
            sns.barplot(x=df[cat], y=df[con], data=df)
            plt.subplot(1,3,2)
            sns.stripplot(x=df[cat], y=df[con], data=df)
            plt.subplot(1,3,3)
            sns.boxplot(x=df[cat], y=df[con], data=df)
            return plt.show()


def months_to_years(telco):
    telco["tenure_years"] = (round(telco.tenure/12)).astype(int)
    return telco