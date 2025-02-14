import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

class MovieRecommender:
    def __init__(self, content_df_path):
        """초기화: 영화 데이터 로드 및 TF-IDF, 코사인 유사도 계산"""
        self.content_df = pd.read_csv(content_df_path)

        # TF-IDF 벡터화
        self.tfidf = TfidfVectorizer(stop_words='english', min_df=5)
        self.tfidf_matrix = self.tfidf.fit_transform(self.content_df['bag_of_words'])

        # 코사인 유사도 계산
        self.cos_sim = cosine_similarity(self.tfidf_matrix)

    def predict(self, title, similarity_weight=0.7, top_n=10):
        """영화 추천 모델 실행"""
        data = self.content_df.reset_index()
        index_movie = data[data['original_title'] == title].index
        if index_movie.empty:
            return f"❌ '{title}' 영화를 찾을 수 없습니다."

        similarity = self.cos_sim[index_movie].T
        sim_df = pd.DataFrame(similarity, columns=['similarity'])
        final_df = pd.concat([data, sim_df], axis=1)

        final_df['final_score'] = final_df['score'] * (1 - similarity_weight) + final_df['similarity'] * similarity_weight
        final_df_sorted = final_df.sort_values(by='final_score', ascending=False).head(top_n)

        return final_df_sorted[['original_title', 'score', 'similarity', 'final_score']]