{% snapshot review_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='id',
          updated_at='scraped_date'
        )
    }}

select id,scraped_date, number_of_reviews,REVIEW_SCORES_RATING,REVIEW_SCORES_ACCURACY,REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN,REVIEW_SCORES_COMMUNICATION,REVIEW_SCORES_VALUE from {{ source('raw', 'house') }}

{% endsnapshot %}