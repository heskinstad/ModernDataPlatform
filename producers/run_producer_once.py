from weather_producer import WeatherProducer


def main():
    p = WeatherProducer()
    p.run_count(count=200, interval_secs=0.01)


if __name__ == "__main__":
    main()
