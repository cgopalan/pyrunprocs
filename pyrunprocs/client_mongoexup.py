import mongoexup as meu


def profile_pre_hook():
    pass


def profile_post_hook():
    pass


profile_fields = (
            "_id,signup_time,optout_time,first_purchase_time,"
            "purchase_time,purchase_count,purchase_price,horizon_count,"
            "lifetime_click,click_time,lifetime_open,open_time,"
            "email,horizon,lists,vars,daily_message,daily_open,daily_pv,"
            "daily_click,lifetime_message,browser,"
            "browser_site,geo.city,purchase_incomplete.qty,"
            "purchase_incomplete.price,purchase_incomplete.title,"
            "purchase_incomplete.id,purchase_incomplete.url,"
            "purchase_incomplete.items,purchases._id,purchases.price,"
            "purchases.qty,purchases.time,purchases.message_id,"
            "purchases.items,horizon_time,email_hour,site_hour,"
            "mobile_email_hour,mobile_site_hour,status,optout_reason"
        )


def profile_pre_hook(client_id, is_partitioned, is_sharded, profile_rs):
    SAILTHRU_DB = "sailthru"
    if is_partitioned:
        query = "{}"
        collection = "profile.{0}".format(client_id)
        db_name = SAILTHRU_DB
    elif is_sharded:
        query = "{client_id : " + str(client_id) + "}"
        collection = "profile"
        db_name = profile_rs
    else:
        query = "{client_id : " + str(client_id) + "}"
        collection = "profile"
        db_name = SAILTHRU_DB
    return query, collection, db_name


task_list = [
                {'operation': meu.EX_EXPORT,
                 'args': ['--host', 'localhost:27017', '--db', 'profileC', '--collection', 'profile',
                        '--fields', profile_fields, '--query', '{client_id: 4320}', '--out', 'profile.json'],
                 'pre_hook' : profile_pre_hook,
                 'post_hook': profile_post_hook},

            ]

def test_mongoexup():
    meu.run(task_list)

if __name__ == '__main__':
    print("Running mongoexup")
    test_mongoexup()

