from prefect import task, Flow, Client

@task
def say_hello():
    print("Hi prefect ...")


with Flow("myflow") as flow:
    plop = say_hello()




if __name__ == "__main__":
    # Run the flow from current python program
    #flow.run()

    # Register the flow inside prefect server
    registration = flow.register("plop")

    # Trigger flow run by prefect server
    client = Client()
    query = client.graphql({'query': {'flow(where: {archived: {_eq: false}})': ['id', 'name']}})
    print(query)

    flow_id = query['data']['flow'][0]['id']
    r = client.create_flow_run(flow_id=query['data']['flow'][0]['id'])
    print(r)


