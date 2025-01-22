from EDA import merged_df
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


agg_data = merged_df.groupby(['shooterPlayerId', 'shot_game_ID']).agg(
    total_shots = ('shotID', 'count'),
    total_goals = ('goal', 'sum'),
    avg_xGoal = ('xGoal', 'mean'),
    is_home_team = ('isHomeTeam','max')
).reset_index()

X = agg_data[['total_shots', 'avg_xGoal', 'is_home_team']]
y = agg_data['total_goals']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=30)

preprocesser = ColumnTransformer(
    transformers = [
        ('num', StandardScaler(), ['total_shots', 'avg_xGoal']),
        ('cat', OneHotEncoder(), ['is_home_team'])
    ]
)

lin_model = Pipeline(steps=[
    ('preprocsser', preprocesser),
    ('regressor', LinearRegression())
])

lin_model.fit(X_train, y_train)

pred_train_lin = lin_model.predict(X_train)

pred_test_lin = lin_model.predict(X_test)

mae_train = mean_absolute_error(y_train, pred_train_lin)
mse_train = mean_squared_error(y_train, pred_train_lin)
r2_train = r2_score(y_train, pred_train_lin)

mae_test = mean_absolute_error(y_test, pred_test_lin)
mse_test = mean_squared_error(y_test, pred_test_lin)
r2_test = r2_score(y_test, pred_test_lin)

print(f"Training Data:\n MAE: {mae_train:.2f}, MSE: {mse_train:.2f}, R²: {r2_train:.2f}")
print(f"Testing Data:\n MAE: {mae_test:.2f}, MSE: {mse_test:.2f}, R²: {r2_test:.2f}")


