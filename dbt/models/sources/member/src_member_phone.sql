select * replace(regexp_replace(phone, r"[() -]", "") as phone)
from {{ source('member_index', 'phone') }} p
