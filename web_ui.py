import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

NUM_RESULTS = 100

@st.cache(show_spinner=False)
def load_tables():
    cosine_table = pd.read_csv('./data/scores/cosine.csv', index_col=0)
    clustering_table = pd.read_csv('./data/scores/wikiClusters.csv', index_col=0)
    link_similarity_table = pd.read_csv('./data/scores/linkSim.csv', index_col=0)
    popularity_table = pd.read_csv('./data/scores/popularityScore.csv', index_col=0)

    tables = [cosine_table, clustering_table, link_similarity_table, popularity_table]

    for table in tables:
        table.index = table.index.str.strip()
        table.columns = table.columns.str.strip()

    return tables


def get_row_intersection(tables):
    row_sets = [set(table.index) for table in tables]
    row_intersection = list(row_sets.pop().intersection(*row_sets))
    return row_intersection


def get_weights():
    columns = st.columns(4)

    with columns[0]:
        cosine_weight = st.slider(label='Cosine Similarity Weight', value=1.0,
                                  min_value=0.0, max_value=1.0, step=0.01, format='')
    with columns[1]:
        clustering_weight = st.slider(label='Clustering Weight', value=1.0,
                                      min_value=0.0, max_value=1.0, step=0.01, format='')
    with columns[2]:
        link_weight = st.slider(label='Link Similarity Weight', value=1.0,
                                min_value=0.0, max_value=1.0, step=0.01, format='')
    with columns[3]:
        popularity_weight = st.slider(label='Popularity Weight', value=1.0,
                                      min_value=0.0, max_value=1.0, step=0.01, format='')

    return {'Cosine Similarity': cosine_weight,
            'Clustering': clustering_weight,
            'Link Similarity': link_weight,
            'Popularity': popularity_weight}


def get_weighted_sum(recommendations: pd.DataFrame, weights: dict):
    weighted_sum = pd.Series([0] * len(recommendations.index), index=recommendations.index)
    for column in recommendations.columns:
        weighted_sum = weighted_sum + weights[column] * recommendations[column]

    return weighted_sum


def format_recommendations(recommendations, article):
    recommendations.index.name = 'Article'
    recommendations.sort_values(by=['Recommendation Score', 'Article'], inplace=True, ascending=[False, True])

    if article in recommendations.index:
        recommendations.drop(article, inplace=True)

    recommendations = recommendations.head(NUM_RESULTS)

    return recommendations


def main():
    st.set_page_config(page_title='WikiRecommend', page_icon='./favicon.png', layout="wide")
    st.title('Wikipedia Recommendations')
    st.text('CS 435 Term Project')
    st.text('By Jake Barth, Preston Dunton, Kaleb Joshi, Matt Whitehead')

    tables = load_tables()

    articles = sorted(get_row_intersection(tables))
    articles.insert(0, '')
    article = st.selectbox(label='Current Article', options=articles)

    weights = get_weights()

    if article != '':

        scores = pd.DataFrame([table.loc[article.strip()] for table in tables])

        scores.index = ['Cosine Similarity', 'Clustering', 'Link Similarity', 'Popularity']

        recommendations = scores.dropna(axis=1).transpose()

        recommendations['Recommendation Score'] = get_weighted_sum(recommendations, weights)

        st.subheader('Recommendations')
        recommendations = format_recommendations(recommendations, article)
        st.dataframe(recommendations.style.background_gradient(cmap=plt.cm.Blues, high=0.35))

        st.subheader('Metric Correlations')
        correlations = recommendations.corr()
        st.dataframe(correlations.style.background_gradient(cmap=plt.cm.Blues, high=0.35))


if __name__ == '__main__':
    main()
