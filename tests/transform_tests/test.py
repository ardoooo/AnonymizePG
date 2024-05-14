# postgres_test1.execute("SELECT * FROM _transfer_workers;")
#     test1_columns = [desc[0] for desc in postgres_test1.description]
#     test1_data = list(postgres_test1.fetchall())
#     utils.eval_shuffle(
#         prod_columns,
#         prod_data,
#         test1_columns,
#         test1_data,
#         [["salary"], ["name", "address"]],
#         5,
#     )

#     if cnt_dests == 2:
#         postgres_test2.execute("SELECT * FROM _transfer_workers;")
#         test2_columns = [desc[0] for desc in postgres_test2.description]
#         test2_data = list(postgres_test2.fetchall())
#         utils.eval_shuffle(
#             prod_columns,
#             prod_data,
#             test2_columns,
#             test2_data,
#             [["salary"], ["name", "address"]],
#             5,
#         )
