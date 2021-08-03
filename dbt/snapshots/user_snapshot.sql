{% snapshot user_snapshot %}

    {{
        config(
          target_database= 'cityblock-analytics',
          target_schema='snapshots',
          unique_key='id',
          strategy= 'check',
          check_cols=['firstName', 'lastName', 'userRole', 'email', 'homeClinicId', 'athenaProviderId', 'locale', 'phone', 'permissions', 'npi']
        )
    }}
    
    -- Pro-Tip: Use sources in snapshots!
    select * from {{ source('commons', 'user') }}
    
{% endsnapshot %}