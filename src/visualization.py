import matplotlib.pyplot as plt
import pandas as pd


def compute_buy_and_hold_nav(prices_df, capital=10_000, price_col='Close'):
    cash = capital
    shares = 0
    nav_history = []

    if prices_df.empty:
        return pd.DataFrame(columns=['Date', 'NAV'])

    first_price = prices_df.iloc[0][price_col]
    if first_price > 0:
        shares = cash // first_price
        cash -= shares * first_price

    for _, row in prices_df.iterrows():
        nav_value = cash + shares * row[price_col]
        nav_history.append({'Date': row['Date'], 'NAV': nav_value})

    return pd.DataFrame(nav_history)


def plot_results(prices_df, benchmark_df, orders, portfolio, config, strategy_nav=None, buy_hold_nav=None, benchmark_nav=None, capital=10_000, price_col='Close'):
    prices_df = prices_df.sort_values('Date').reset_index(drop=True)
    benchmark_df = benchmark_df.sort_values('Date').reset_index(drop=True)

    ticker = config.get('ticker', 'UNKNOWN')
    benchmark_ticker = config.get('benchmark_ticker', 'SPY')
    start_date = config.get('start_date', 'UNKNOWN')
    end_date = config.get('end_date', 'UNKNOWN')

    if buy_hold_nav is None:
        buy_hold_nav = compute_buy_and_hold_nav(prices_df, capital=capital, price_col=price_col)

    fig, (ax_price, ax_nav) = plt.subplots(2, 1, figsize=(12, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    fig.patch.set_facecolor('black')

    x = pd.to_datetime(prices_df['Date'])
    ax_price.plot(x, prices_df[price_col], color="#E4E9B8", label=f"{ticker} Price", zorder=1)

    if not orders.empty:
        buys = orders[orders['Action'] == 'Buy']
        sells = orders[orders['Action'] == 'Sell']
        ax_price.scatter(pd.to_datetime(buys['Date']), buys['Close'], marker='^', color='green', s=80, label='Buy', zorder=2)
        ax_price.scatter(pd.to_datetime(sells['Date']), sells['Close'], marker='v', color='red', s=80, label='Sell', zorder=2)

    if strategy_nav is not None:
        ax_nav.plot(pd.to_datetime(strategy_nav['Date']), strategy_nav['NAV'], color='#7FDBFF', label='Strategy NAV', linewidth=2)
    if buy_hold_nav is not None:
        ax_nav.plot(pd.to_datetime(buy_hold_nav['Date']), buy_hold_nav['NAV'], color='#2ECC40', label='Buy-and-Hold NAV', linewidth=2)
    if benchmark_nav is not None:
        ax_nav.plot(pd.to_datetime(benchmark_nav['Date']), benchmark_nav['NAV'], color='#FFDC00', label=f"{benchmark_ticker} Buy-and-Hold NAV", linewidth=2)

    ax_price.set_facecolor('black')
    ax_price.set_title(f"Backtested {ticker} Prices from {start_date} to {end_date}", color='white', fontname='serif')
    ax_price.set_ylabel('Price (USD)', color='white', fontname='serif')
    ax_price.tick_params(axis='x', colors='white')
    ax_price.tick_params(axis='y', colors='white')
    price_legend = ax_price.legend(facecolor='black', framealpha=0.7, edgecolor='white', loc='upper left')
    for text in price_legend.get_texts():
        text.set_color('white')

    # Add PNL metrics as text in the bottom-right corner of the price chart
    pnl_text = f'Buy Orders: {portfolio.buy_orders}\nSell Orders: {portfolio.sell_orders}\nTotal Profit: ${portfolio.total_profit:.2f}\nPercent Gain: {portfolio.percent_gain:.2f}%'
    ax_price.text(0.98, 0.02, pnl_text, transform=ax_price.transAxes, fontsize=10, 
                  verticalalignment='bottom', horizontalalignment='right',
                  bbox=dict(boxstyle='round,pad=0.5', facecolor='black', edgecolor='white', alpha=0.8),
                  color='white', fontname='serif')

    ax_nav.set_facecolor('black')
    ax_nav.set_ylabel('NAV (USD)', color='white', fontname='serif')
    ax_nav.set_xlabel('Date', color='white', fontname='serif')
    ax_nav.tick_params(axis='x', colors='white')
    ax_nav.tick_params(axis='y', colors='white')
    nav_legend = ax_nav.legend(facecolor='black', framealpha=0.7, edgecolor='white')
    for text in nav_legend.get_texts():
        text.set_color('white')

    for ax in (ax_price, ax_nav):
        for spine in ax.spines.values():
            spine.set_color('white')

    fig.tight_layout()
    plt.show()
