#!/usr/bin/env python
# coding: utf-8

# In[1]:


'''
!pip install alpaca_trade_api
!pip install pandas_ta
!pip install plotly
!pip install backtesting
!pip install ta
!pip install --upgrade backtesting bokeh
!pip install seaborn
'''


# In[2]:


import os
'''
import torch
import torch.nn as nn
import torch.optim as optim
'''
import numpy as np
import pandas as pd
import pandas_ta as ta
import datetime
import pickle
from scipy.signal import argrelextrema
import warnings

# Suppress specific warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)


# # Define Chart_Specs Object

# In[3]:


class Chart_Specs:

    def __init__(self, chart_type ,symbol, timeframe, start_date , end_date , drive = None , subfolder=None , verbose = False):

        # Input Data Passed
        self.chart_type = chart_type
        self.symbol = symbol
        self.timeframe = timeframe
        self.start_date = start_date
        self.end_date = end_date
        self.drive = drive
        self.subfolder = subfolder

        # Derived Folder Name Data
        self.chart_folder_name = self.get_chartName()
        self.timeframe_folder_name = self.get_timeframeName()
        self.date_range_folder_name = self.get_date_range_folder_name()

        # Derived Data
        self.timeframe_in_hours = self.get_timeframe_in_hours(timeframe)

        # Final Folder Path
        self.final_folder_path = self.get_final_folder_path()

        # Create Final Folder
        if not os.path.exists(self.final_folder_path):
            os.makedirs(self.final_folder_path, exist_ok = True)

        # Create Chart_Specs Class Pickle File
        self.Chart_Specs_path = self.final_folder_path + 'Chart_Specs.pkl'
        self.save_to_file(self.Chart_Specs_path)

        # Print Details
        self.verbose = verbose
        if self.verbose:
          self.print_details()

    def save_to_file(self, filename):
        """Save the instance to a file."""
        with open(filename, 'wb') as file:
            pickle.dump(self, file)

    @classmethod
    def load_from_file(cls, filename):
        """Load an instance from a file."""
        with open(filename, 'rb') as file:
            instance = pickle.load(file)
        return instance

    def get_chartName(self):
        return self.symbol.replace('/', '-')

    def get_timeframeName(self):
        return str('T-'+self.timeframe)

    def get_timeframe_in_hours(self,t):
      match t:
        case '1Min':
          return 60/1
        case '5Min':
          return  60/5
        case '15Min':
          return  60/15
        case '30Min':
          return  60/30
        case '1H':
          return  60/60
        case '2H':
          return  60/120
        case '4H':
          return  60/240
        case '1D':
          return  60/1440
        case '1W':
          return  60/10080
        case '1M':
          return  60/43200

    def get_date_range_folder_name(self):
      # Convert datetimes to strings in a folder-name-friendly format
      start_str = self.start_date.strftime('%Y%m%d-%H%M%S')
      end_str = self.end_date.strftime('%Y%m%d-%H%M%S')
      date_range_folder_name = f"{start_str}-to-{end_str}"
      return str(date_range_folder_name)

    def get_final_folder_path(self):
        if self.drive == None:
            if self.subfolder == None:
                return f"{self.chart_type}/{self.chart_folder_name}/{self.timeframe_folder_name}/{self.date_range_folder_name}/"
            else:
                return f"{self.subfolder}/{self.chart_type}/{self.chart_folder_name}/{self.timeframe_folder_name}/{self.date_range_folder_name}/"
        else:
            self.drive_path = f"{self.drive}:"
            if self.subfolder == None:
                return f"{self.drive_path}/{self.chart_type}/{self.chart_folder_name}/{self.timeframe_folder_name}/{self.date_range_folder_name}/"
            else:
                return f"{self.drive_path}/{self.subfolder}/{self.chart_type}/{self.chart_folder_name}/{self.timeframe_folder_name}/{self.date_range_folder_name}/"
    
    def get_Chart_Specs_path(self):
      return self.Chart_Specs_path

    def print_details(self):
      print('__________ Input Details __________')
      print('Chart Type :',self.chart_type)
      print('Symbol :',self.symbol)
      print('Timeframe :',self.timeframe)
      print('Start Date :',self.start_date)
      print('End Date :',self.end_date)
      print('__________ Derived Details __________')
      print('Chart Name :',self.chart_folder_name)
      print('Timeframe Name :',self.timeframe_folder_name)
      print('Date Range Folder Name :',self.date_range_folder_name)
      print('Final Folder Path :',self.final_folder_path)
      print('Timeframe in Hours :',self.timeframe_in_hours)


# # Define Fetch_Data_Specs
# 

# In[4]:


from warnings import WarningMessage
class Fetch_Data_Specs:

    def __init__(self , platform_name , key = None , secret = None , url = None , chart_specs = None , verbose = False , sensitive_verbose = False ):

      # Input Data
      self.platform_name = platform_name
      self.key = key
      self.secret = secret
      self.url = url
      self.chart_specs = chart_specs
      self.verbose = verbose
      self.sensitive_verbose = sensitive_verbose
      self.historical_data = None
      self.dir_path =  None
      self.historical_data_path = None
      self.Fetch_Data_Specs_file_path = None
      # Derived Data


      # Print Details
      if self.verbose:
        self.print_details()

    def run(self):
      # HISTORICAL DATA
      if self.verbose:
        print('Fetching Historical Data')
      self.get_historical_data()

      # SAVE HISTORICAL DATA
      if self.verbose:
        print('Fetching Historical Data')
      self.save_historical_data()

      # Save Fetch_Data_Specs
      if self.chart_specs is not None:
        self.Fetch_Data_Specs_file_path = self.chart_specs.final_folder_path +'Fetch_Data_Specs.pkl'
        self.save_to_file(self.Fetch_Data_Specs_file_path)

    def save_to_file(self, filename):
        """Save the instance to a file."""
        with open(filename, 'wb') as file:
            pickle.dump(self, file)
        print('Fetch_data_Specs Instance saved to', filename)

    @classmethod
    def load_from_file(cls, filename):
        """Load an instance from a file."""
        with open(filename, 'rb') as file:
            instance = pickle.load(file)
        print('Fetch_data_Specs Instance loaded from', filename)
        return instance

    def print_details(self):
      print('__________ Input Details __________')
      print('Platform Name :',self.platform_name)
      if self.sensitive_verbose:
        print('Key :',self.key)
        print('Secret :',self.secret)
        print('Url :',self.url)
      if self.chart_specs is not None:
        print('Chart Specs Name:',self.chart_specs.chart_folder_name)
        print('Chart Specs Symbol:',self.chart_specs.symbol)
        print('Chart Specs Timeframe:',self.chart_specs.timeframe)
        print('Chart Specs Start Date:',self.chart_specs.start_date)
        print('Chart Specs End Date:',self.chart_specs.end_date)
      else:
        print('Chart Specs :',self.chart_specs)
      print('__________ Derived Details __________')
      print('Directory Path :',self.dir_path)
      print('Historical Data Path :',self.historical_data_path)
      if self.sensitive_verbose:
        print('Historical Data :',self.historical_data)
      if self.Fetch_Data_Specs_file_path is not None:
        print('Fetch Data Specs File Path :',self.Fetch_Data_Specs_file_path)

    def add_credentials(self, key, secret, url):
      self.key = key
      self.secret = secret
      self.url = url
      if self.verbose:
        print('Credentials Added')
        if self.sensitive_verbose:
          print('Key :',self.key)
          print('Secret :',self.secret)
          print('Url :',self.url)

    def add_chart_specs(self, chart_specs):
      self.chart_specs = chart_specs
      # Save Fetch_Data_Specs
      self.Fetch_Data_Specs_file_path = None
      if self.chart_specs is not None:
        self.Fetch_Data_Specs_file_path = self.chart_specs.final_folder_path +'Fetch_Data_Specs.pkl'
        self.save_to_file(self.Fetch_Data_Specs_file_path)

    def save_historical_data(self):
      if self.historical_data is None:
        self.get_historical_data()
      if self.historical_data is not None:
        self.dir_path =  self.chart_specs.final_folder_path
        # Creating intermediate directories
        import os
        os.makedirs(self.dir_path, exist_ok=True)
        self.historical_data_path = self.dir_path + 'historical_data.csv'
        # Saving Historical Data
        self.historical_data.to_csv(self.historical_data_path)
        if self.verbose:
          print('Historical Data Saved to ',self.historical_data_path)
      else:
        print('Historical Data Not Saved  since it was not created')

    def get_historical_data(self):
      if self.chart_specs is not None:

        # Basic Data
        chart_type = self.chart_specs.chart_type
        symbol = self.chart_specs.symbol
        timeframe = self.chart_specs.timeframe
        start_date = self.chart_specs.start_date
        end_date = self.chart_specs.end_date

        # Match to Appropriate Platform
        match self.platform_name:

          # ALPACA Platform
          case 'alpaca':
            if self.key is not None and self.secret is not None and self.url is not None:
              # Creating Alpaca Connection
              import alpaca_trade_api as tradeapi
              api = tradeapi.REST(self.key, self.secret, self.url)
              # Get Crypto historical data
              match self.chart_specs.chart_type:
                # Crypto Historical Data
                case 'crypto':
                  self.historical_data = api.get_crypto_bars(symbol, timeframe, start=start_date.isoformat(), end=end_date.isoformat(), limit=None).df
                  # Drop column 'symbol' since it is text
                  self.historical_data = self.historical_data.drop(['symbol'], axis = 1)
                  # Rearrange columns to set format
                  self.historical_data = self.historical_data.reindex(['open','high','low','close','volume','vwap','trade_count'], axis=1)
                # Stocks Historical Data
                case 'stocks':
                  self.historical_data = api.get_bars(symbol, timeframe, start=start_date.isoformat(), end=end_date.isoformat(), limit=None).df
                  # Rearrange columns to set format
                  self.historical_data = self.historical_data.reindex(['open','high','low','close','volume','vwap','trade_count'], axis=1)
                # Forex Historical Data
                case 'forex':
                  self.historical_data = api.get_bars(symbol, timeframe, start=start_date.isoformat(), end=end_date.isoformat(), limit=None).df

                case _:
                  print('Errorg Message : Cannot create Historical Data since chart_type not identified')


            else:
              print('Errorg Message : Cannot create Historical Data since Credentials are not yet Defined')
              print('Reasons:')
              counter = 0
              if self.key is None:
                counter += 1
                print(str(counter),'. key is not defined')
              if self.secret is None:
                counter += 1
                print(str(counter),'. secret is not defined')
              if self.url is None:
                counter += 1
                print(str(counter),'. url is not defined')
              print('Please complete the above ',str(counter),' Requirements' )

          case _:
            print('Errorg Message : Cannot create Historical Data since platform_name not identified')

      else:
        print('Errorg Message : Cannot create Historical Data since Chart_Specs is not added')

      if self.verbose:
        if self.historical_data is not None:
          print('Historical Data Created Successfully')
          print('Historical Data Shape : ',self.historical_data.shape)

        else:
          print('Historical Data Not Created')






#  # Define Feat_Eng Object

# In[5]:


import os
import warnings

"""
class Feat_Eng:
  def __init__(self, dir_path = None ,df = None , indicators_list=[] , trend_signals_list=[], TA_indicators_parameters=None , verbose = False):

    # Default Existing Indicator Lists
    self.vma_list = []
    self.ma_list = []
    self.ema_list = []
    self.rsi_list = []
    self.tr_list = []
    self.atr_list = []
    self.vwap_list = []
    self.bb_list = []

    # Normalized Indicator Lists
    self.norm_indicators_list = []
    self.norm_vma_list = []
    self.norm_ma_list = []
    self.norm_ema_list = []
    self.norm_rsi_list = []
    self.norm_tr_list = []
    self.norm_atr_list = []
    self.norm_vwap_list = []
    self.norm_bb_list = []

    # Default Trend Indicator Lists
    self.TS_list=[]
    self.SoloEMA_TrendSignal_list = []
    self.EMACrossover_TrendSignal_list = []

    # Default Reversal Indicator Lists
    self.RS_list=[]
    self.rsi_reversal_list =[]
    self.bb_reversal_list =[]
    self.price_action_reversal_list=[]
    

    # Default Tot Signals Lists
    self.TotSignal_list = []
    self.TotSignal_Trade_Counts = []
    self.TotSignal_Long_Counts = []
    self.TotSignal_Short_Counts = []


    # Input Data
    self.dir_path = dir_path
    self.df = df
    self.indicators_list = indicators_list
    self.trend_signals_list = trend_signals_list
    self.verbose = verbose

    # Derived Data
    if self.dir_path != None:
        self.Feat_Eng_file_path = self.dir_path + 'Feat_Eng.pkl'
    else:
        self.Feat_Eng_file_path = 'Feat_Eng.pkl'


    # Use default if nothing passed
    default_params = {
        "ema_periods": [9, 12, 21, 26, 50, 100, 200],
        "rsi_periods": [9, 10, 14, 20, 25],
        "atr_periods": [7, 10, 14, 20, 30],
        "backcandles_count": [1, 2, 3, 4, 5],
        "rsi_sensitivity": [5, 10, 15]
    }
    self.TA_indicators_parameters = TA_indicators_parameters or default_params
    
    # Assign individual sets for convenience
    self.ema_periods = self.TA_indicators_parameters['ema_periods']
    self.rsi_periods = self.TA_indicators_parameters['rsi_periods']
    self.atr_periods = self.TA_indicators_parameters['atr_periods']
    self.backcandles_count = self.TA_indicators_parameters['backcandles_count']
    self.rsi_sensitivity = self.TA_indicators_parameters['rsi_sensitivity']
    

  def get_min_max_values_of_columns(self,columns):
      max_values, min_values = [],[]
      for i in columns:
          max = self.df.max(axis=0)[i]
          min = self.df.min(axis=0)[i]
          max_values.append(max)
          min_values.append(min)
      return max_values,min_values
  
  def flatten_list(self,nested_list):
    result = []
    for element in nested_list:
        if isinstance(element, list):  # Check if the item is a list
            result.extend(self.flatten_list(element))  # Recurse into the sublist
        else:
            result.append(element)  # Add the non-list item to the result
    return result
      
  def print_indicator_details(self):
      print('_____________________________________________________')
      print('__________ Indicator Details ________________________')
      print('_____________________________________________________')
      print('Volume Moving Average (VMA) ::')
      print('Count:',len(self.vma_list))
      print('VMA List:   ',self.vma_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.vma_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('Moving Averages (MA) ::')
      print('Count:',len(self.ma_list))
      print('MA List:   ',self.ma_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.ma_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('Exponential Moving Averages (EMA) ::')
      print('Count:',len(self.ema_list))
      print('EMA List:  ',self.ema_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.ema_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('Relative Strength Index (RSI) ::')
      print('Count:',len(self.rsi_list))
      print('RSI List:  ',self.rsi_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.rsi_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('True Range (TR) ::')
      print('Count:',len(self.tr_list))
      print('TR List:   ',self.tr_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.tr_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('Average True Range (ATR) ::')
      print('Count:',len(self.atr_list))
      print('ATR List:   ',self.atr_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.atr_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('Volume Weighted Average Price (VWAP) ::')
      print('Count:',len(self.vwap_list))
      print('VWAP List:   ',self.vwap_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.vwap_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('Bollinger Bands (BB) ::')
      print('Count:',len(self.bb_list))
      print('BB List:   ',self.bb_list)
      max_array , min_array=[],[]
      for bb in self.bb_list:
          max_values, min_values = self.get_min_max_values_of_columns(bb)
          max_array.append(max_values)
          min_array.append(min_values)
      print('MAX Values:',max_array)
      print('MIN Values:',min_array)
      print('_____________________________________________________')

    
  def print_normalized_indicator_details(self):
      print('_____________________________________________________')
      print('_________ Normalized Indicator Details ______________')
      print('_____________________________________________________')
      print('Volume Moving Average (VMA) ::')
      print('Count:',len(self.norm_vma_list))
      print('Normalised VMA List:   ',self.norm_vma_list)
      max_array , min_array=[],[]
      for norm_vma in self.norm_vma_list:
          max_values, min_values = self.get_min_max_values_of_columns(norm_vma)
          max_array.append(max_values)
          min_array.append(min_values)
      print('MAX Values:',max_array)
      print('MIN Values:',min_array)
      print('_____________________________________________________')
      print('Moving Averages (MA) ::')
      print('Count:',len(self.norm_ma_list))
      print('Normalised MA List:   ',self.norm_ma_list)
      max_array , min_array=[],[]
      for norm_ma in self.norm_ma_list:
          max_values, min_values = self.get_min_max_values_of_columns(norm_ma)
          max_array.append(max_values)
          min_array.append(min_values)
      print('MAX Values:',max_array)
      print('MIN Values:',min_array)
      print('_____________________________________________________')
      print('Exponential Moving Averages (EMA) ::')
      print('Count:',len(self.norm_ema_list))
      print('Normalised EMA List:  ',self.norm_ema_list)
      max_array , min_array=[],[]
      for norm_ema in self.norm_ema_list:
          max_values, min_values = self.get_min_max_values_of_columns(norm_ema)
          max_array.append(max_values)
          min_array.append(min_values)
      print('MAX Values:',max_array)
      print('MIN Values:',min_array)
      print('_____________________________________________________')
      print('Relative Strength Index (RSI) ::')
      print('Count:',len(self.norm_rsi_list))
      print('Normalised RSI List:  ',self.norm_rsi_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.norm_rsi_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('True Range (TR) ::')
      print('Count:',len(self.norm_tr_list))
      print('Normalised TR List:   ',self.norm_tr_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.norm_tr_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('Average True Range (ATR) ::')
      print('Count:',len(self.norm_atr_list))
      print('Normalised ATR List:   ',self.norm_atr_list)
      max_values, min_values = self.get_min_max_values_of_columns(self.norm_atr_list)
      print('MAX Values:',max_values)
      print('MIN Values:',min_values)
      print('_____________________________________________________')
      print('Volume Weighted Average Price (VWAP) ::')
      print('Count:',len(self.norm_vwap_list))
      print('Normalised VWAP List:   ',self.norm_vwap_list)
      max_array , min_array=[],[]
      for norm_vwap in self.norm_vwap_list:
          max_values, min_values = self.get_min_max_values_of_columns(norm_vwap)
          max_array.append(max_values)
          min_array.append(min_values)
      print('MAX Values:',max_array)
      print('MIN Values:',min_array)
      print('_____________________________________________________')
      print('Normalized Bollinger Bands (BB) ::')
      print('Count:',len(self.norm_bb_list))
      print('Norm BB List:   ',self.norm_bb_list)
      max_array , min_array=[],[]
      for norm_bb in self.norm_bb_list:
          max_values, min_values = self.get_min_max_values_of_columns(norm_bb)
          max_array.append(max_values)
          min_array.append(min_values)
      print('MAX Values:',max_array)
      print('MIN Values:',min_array)
      print('_____________________________________________________')
      
      
  def run(self,Indicators = True, Normalizations =True , Trend_Signals = True, Reversal_Signals=True , Tot_Signals = True, Save = True):

    # Rename Columns to Standard Format
    self.rename_columns()

    # Apply Indicators
    if Indicators:
        if self.verbose:
            print('Begin to Processing Indicators')
            print('_____________________________________________________')
        self.apply_indicators()
        if Normalizations:
            if self.verbose:
                print('Begin to Normalize Indicators')
                print('_____________________________________________________')
            self.normalize_indicators()
        if self.verbose:
            print('Completed Processing Indicators')
            print('_____________________________________________________')

    # Apply Trend Signals
    if Trend_Signals:
        if self.verbose:
            print('Begin to Processing Trend Signals')
            print('_____________________________________________________')
        self.apply_trend_signals()


    # Reversal Signals
    if Reversal_Signals:
        if self.verbose:
            print("Processing Reversal Signals...")
        self.apply_reversal_signals()

    # Apply Tot Signals
    if Tot_Signals:
        if self.verbose:
            print('Begin to Processing Tot Signals')
            print('_____________________________________________________')
        self.apply_Tot_signals()

    # Rank Tot Signals
    self.rank_TotSignal()

    # All Explored TotSignals Info
    self.get_explored_signals_df()

    if Save:
        # Save Data
        self.save_data()
    
        # Save Feat_Eng instance
        self.save_to_file(self.Feat_Eng_file_path)

  def save_to_file(self, filename):
      with open(filename, 'wb') as file:
        pickle.dump(self, file)
      if self.verbose:
        print('Feat_Eng Instance saved to', filename)

  @classmethod
  def load_from_file(cls, filename):
      with open(filename, 'rb') as file:
        instance = pickle.load(file)
      print('Feat_Eng Instance loaded from', filename)
      return instance


  def save_data(self):
      # Creating intermediate directories
      os.makedirs(self.dir_path, exist_ok=True)

      # Saving Feature Endineered Data
      if self.df is not None:
        self.feat_data_path = self.dir_path + 'feat_data.csv'
        self.df.to_csv(self.feat_data_path)
        if self.verbose:
          print('Feature Engineered Data Saved to ',self.feat_data_path)
      else:
        print('Feature Engineered Data Not Saved  since it was not created')

      # Saving Explored Signals Data
      if self.explored_signals_df is not None:
        self.explored_signals_path = self.dir_path + 'explored_signals.csv'
        self.explored_signals_df.to_csv(self.explored_signals_path)
        if self.verbose:
          print('Explored Signals Data Saved to ',self.explored_signals_path)
      else:
        print('Explored Signals Data Not Saved  since it was not created')

  def rank_TotSignal(self):
    #Arrange the Totsignal list in descending order as per Trade Counts
    # Get the indices that would sort the counts array in descending order
    sorted_indices = np.argsort(self.TotSignal_Trade_Counts)[::-1]
    # Sort the names array according to the sorted_indices
    self.TotSignal_list = [self.TotSignal_list[i] for i in sorted_indices]
    self.TotSignal_Trade_Counts = [self.TotSignal_Trade_Counts[i] for i in sorted_indices]
    self.TotSignal_Long_Counts = [self.TotSignal_Long_Counts[i] for i in sorted_indices]
    self.TotSignal_Short_Counts = [self.TotSignal_Short_Counts[i] for i in sorted_indices]
    if self.verbose:
      print('TotSignal list sorted in descending order as per Trade Counts, thus ranking as per Trade Counts')

  def rename_columns(self):
    init_columns = self.df.columns
    new_columns = {}
    if 'open' in init_columns:
      new_columns['open'] = 'Open'
    if 'high' in init_columns:
      new_columns['high'] = 'High'
    if 'low' in init_columns:
      new_columns['low'] = 'Low'
    if 'close' in init_columns:
      new_columns['close'] = 'Close'
    if 'volume' in init_columns:
      new_columns['volume'] = 'Volume'
    if 'vwap' in init_columns:
      new_columns['vwap'] = 'VWAP'
    if 'trade_count' in init_columns:
      new_columns['trade_count'] = 'Trade Count'
    self.df.rename(columns=new_columns, inplace=True)
    if self.verbose:
      n_columns = self.df.columns
      print(f'Columns renamed from {init_columns} to {n_columns}')


  def compute_rsi(self, window=14):
    delta = self.df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

  def compute_TR(self):
    self.df['close_prev'] = self.df['Close'].shift(1)
    self.df['high-low'] = self.df['High'] - self.df['Low']
    self.df['high-close_prev'] = abs(self.df['High'] - self.df['close_prev'])
    self.df['low-close_prev'] = abs(self.df['Low'] - self.df['close_prev'])
    self.df['TR'] = self.df[['high-low', 'high-close_prev', 'low-close_prev']].max(axis=1)

  def compute_SoloEMA_TrendSignal(self, EMA_period , backcandles):
    emasignal = [0]*len(self.df)
    for row in range(backcandles-1, len(self.df)):
      upt = 1
      dnt = 1
      for i in range(row-backcandles, row+1):
          if self.df['High'].iloc[i] >= self.df[EMA_period].iloc[i]:
              dnt = 0
          if self.df['Low'].iloc[i] <= self.df[EMA_period].iloc[i]:
              upt = 0
      if upt==1 and dnt==1:
          #print("!!!!! check trend loop !!!!")
          emasignal[row]=3
      elif upt==1:
          #print("!!!!! UPTREND !!!!")
          emasignal[row]=2
      elif dnt==1:
          #print("!!!!! DOWNTREND !!!!")
          emasignal[row]=1
      else:
          #print("!!!!! Trend Reversal !!!!")
          emasignal[row]=0
    return emasignal

  def normalizations_wrt_close(self,C,z_norm_coeff = 1,mr_norm_coeff=1,Zscore=True,PercentDiff=True,FixedRange=True,LogNorm=True,ExpDecay=True,MovingRange=True):
    # C is column name
    normalised_C = []
    # Normalizations
    Diff = self.df['Close'] - self.df[C]
    max_diff = Diff.abs().max()
    max_diff = Diff.abs().max()
    # 1. Standard Score Normalization (Z-Score)
    if Zscore:
        periods = [20,50,100]
        for period in periods:
            z_norm_name = f'ZNorm-{period}_{C}'
            Standard_Deviation = Diff.rolling(window=period).std()
            self.df[z_norm_name] = (Diff / Standard_Deviation)/(100*z_norm_coeff)
            normalised_C.append(z_norm_name)
    # 2. Percentage Difference
    if PercentDiff:
        percent_norm_name = f'PercentNorm_{C}'
        self.df[percent_norm_name] = Diff / self.df[C]
        normalised_C.append(percent_norm_name)
    # 3. Fixed Range Normalization
    if FixedRange:
        fixed_range_norm_name = f'FixedRangeNorm_{C}'
        self.df[fixed_range_norm_name] = Diff / max_diff
        normalised_C.append(fixed_range_norm_name)
    # 4. Logarithmic Return
    if LogNorm:
        log_norm_name = f'LogNorm_{C}'
        self.df[log_norm_name] = np.log(self.df['Close'] / self.df[C])
        normalised_C.append(log_norm_name)
    # 5. Exponential Decay Normalization (with a simple exponential decay example)
    if ExpDecay: 
        alpha = [0.3,0.1,0.05]
        for a in alpha:
            exp_decay_norm_name = f'ExpDecayNorm-{a}_{C}'
            self.df[exp_decay_norm_name] = (np.exp(-a * np.arange(len(self.df))) * Diff)/100
            normalised_C.append(exp_decay_norm_name)
    # 6. Moving Range Normalization
    if MovingRange:
        periods = [20,50,100]
        for n in periods:
            mr_norm_name = f'MovingRangeNorm-{n}_{C}'
            High_n = self.df['Close'].rolling(window=n).max()
            Low_n = self.df['Close'].rolling(window=n).min()
            self.df[mr_norm_name] = (Diff / (High_n - Low_n))/(100*mr_norm_coeff)
            normalised_C.append(mr_norm_name)      
    # Return Results
    return normalised_C
    
  def normalize_indicators(self):
    for indicator in self.indicators_list:

      # Case as per indicators
      match indicator:
        
        # Moving Volume
        case 'VMA':
          for vma in self.vma_list:
              norms=[]
              # Deconstruct
              [_,p]=vma.split('-')
              # Percent Normalisation
              percent_norm_name = f'PercentNorm_{vma}' 
              self.df[percent_norm_name] = (self.df[vma].pct_change())/100
              norms.append(percent_norm_name)
              # Log Normalisation
              log_norm_name = f'LogNorm_{vma}' 
              self.df[log_norm_name] = (np.log(self.df[vma] + 1))/100
              norms.append(log_norm_name)
              # Price Range Normalisation and Ratio Normalisation
              vma_periods = [5,10,20,30,50,100,200]
              for period in vma_periods:
                  # Price Range Normalisation
                  pr_norm_name = f'PriceRangeNorm-{period}_{vma}'
                  Average_Price_Range = (self.df['High'] - self.df['Low']).rolling(window=period).mean()
                  self.df[pr_norm_name] = (self.df[vma] / Average_Price_Range)/1000
                  norms.append(pr_norm_name)
                  # Ratio Normalisation
                  if int(p)<period:
                      ratio_norm_name = f'RatioNorm-{p}-{period}_{vma}'
                      Long_Term_VMA = self.df['Volume'].rolling(window=period).mean().copy(deep=True)
                      self.df[ratio_norm_name] = (self.df[vma].copy(deep=True) / Long_Term_VMA)/100
                      norms.append(ratio_norm_name)
              # Add all VMA Normalisations to norm_vma_list
              self.norm_vma_list .append(norms)
          # Add to Normalised Indicator List
          self.norm_indicators_list.append(self.norm_vma_list)
          # Print Confirmation
          if self.verbose:
            print(f'VMA indicators normalised')
            print(f'Normalised VMA List : {self.norm_vma_list}')
            print('Len Normalised VMA List :',len(self.norm_vma_list))
        
        # Moving Averages
        case 'MA':
          for ma in self.ma_list:
              norms = self.normalizations_wrt_close(ma)
              self.norm_ma_list.append(norms)
          # Add to Normalised Indicator List
          self.norm_indicators_list.append(self.norm_ma_list)
          # Print Confirmation
          if self.verbose:
            print(f'MA indicators normalised')
            print(f'Normalised MA List : {self.norm_ma_list}')
            print('Len Normalised MA List :',len(self.norm_ma_list))
        
        # Exponential Moving Averages
        case 'EMA':
          for ema in self.ema_list:
            norms = self.normalizations_wrt_close(ema)
            # Add to Normalized EMA List
            self.norm_ema_list.append(norms)
          # Add to Normalised Indicator List
          self.norm_indicators_list.append(self.norm_ema_list)
          # Print Confirmation
          if self.verbose:
            print(f'EMA indicators normalized')
            print(f'Normalized EMA List : {self.norm_ema_list}')
            print('Len Nomralized EMA List :',len(self.norm_ema_list))
        
        # Relative Strength Index
        case 'RSI':
          for rsi in self.rsi_list:
            norm_rsi_name = f'Norm_{rsi}'
            self.df[norm_rsi_name] = (self.df[rsi] - 50)/100
            self.norm_rsi_list.append(norm_rsi_name)
          # Add to Normalised Indicator List
          self.norm_indicators_list.append(self.norm_rsi_list)
          # Print Confirmation
          if self.verbose:
            print(f'RSI indicators normalised')
            print(f'Normalised RSI List : {self.norm_rsi_list}')
            print('Len Normalised RSI List :',len(self.norm_rsi_list))
        
        # True Range (TR)
        case 'TR':
          for tr in self.tr_list:
            norms = f'Norm{tr}'
            self.df[norms] = (self.df[tr]/self.df['Close'])/100  
            # Add to Normalized TR List
            self.norm_tr_list.append(norms)
          # Add to Normalised Indicator List
          self.norm_indicators_list.append(self.norm_tr_list)
          # Print Confirmation
          if self.verbose:
            print(f'TR indicators normalised')
            print(f'Normalised TR List : {self.norm_tr_list}')
            print('Len Normalised TR List :',len(self.norm_tr_list))
        
        # Average True Range (ATR)
        case 'ATR':
          for atr in self.atr_list:
            norms = f'Norm{atr}'
            self.df[norms] = (self.df[atr]/self.df['Close'])  
            # Add to Normalized ATR List
            self.norm_atr_list.append(norms)
          # Add to Normalised Indicator List
          self.norm_indicators_list.append(self.norm_atr_list)
          # Print Confirmation
          if self.verbose:
            print(f'ATR indicators normalised')
            print(f'Normalised ATR List : {self.norm_atr_list}')
            print('Len Normalised ATR List :',len(self.norm_atr_list))
        
        # Volume Weighted Average Price (VWAP)
        case 'VWAP':
          for vwap in self.vwap_list:
            # Normalizations W.R.T Close Price
            norms = self.normalizations_wrt_close(vwap,Zscore=False,MovingRange=False)
            # Z score Normalizations and Moving Range Normalizations 
            periods = [20,50,100]
            for period in periods:
                VWAP_Mean = self.df[vwap].rolling(window=period).mean()
                VWAP_Std = self.df[vwap].rolling(window=period).std()
                VWAP_Max = self.df[vwap].rolling(window=period).max()
                VWAP_Min = self.df[vwap].rolling(window=period).min()
                # Apply Z score Normalizations
                z_norm_name = f'ZNorm-{period}_{vwap}'   
                self.df[z_norm_name] = ((self.df[vwap] - VWAP_Mean) / VWAP_Std)/100
                norms.append(z_norm_name)
                # Apply Moving Range Normalizations
                mr_norm_name = f'MovingRangeNorm-{period}_{vwap}'
                self.df[mr_norm_name] = 2 * ((self.df[vwap] - VWAP_Min) / (VWAP_Max - VWAP_Min)) - 1
                norms.append(mr_norm_name)
            # Add to Normalized ATR List
            self.norm_vwap_list.append(norms)
          # Add to Normalised Indicator List
          self.norm_indicators_list.append(self.norm_vwap_list)
          # Print Confirmation
          if self.verbose:
            print(f'VWAP indicators normalised')
            print(f'Normalised VWAP List : {self.norm_vwap_list}')
            print('Len Normalised VWAP List :',len(self.norm_vwap_list))

        # Moving Volume
        case 'BB':
          for bb in self.bb_list:
              # Deconstruct BB
              lower_name,mid_name,upper_name,bandwidth_name,percent_name =bb[0],bb[1],bb[2],bb[3],bb[4]
              [_,period,std_dev] = lower_name.split('_')
              norm_name = f'NormBB-{period}-{std_dev}'
              norm_bandwidth_name = f'NormBBB-{period}-{std_dev}'
              norm_percent_name = f'NormBBP-{period}-{std_dev}'
              # Normalization
              self.df[norm_name] = ((self.df['Close'] - self.df[mid_name]) / (self.df[upper_name] - self.df[lower_name]))/10
              self.df[norm_bandwidth_name] = self.df[bandwidth_name]/100
              self.df[norm_percent_name] = self.df[percent_name]/100
              # Add to Normalized BB List
              self.norm_bb_list.append([norm_name,norm_bandwidth_name,norm_percent_name]) 
          # Add to Normalised Indicator List
          self.norm_indicators_list.append(self.norm_bb_list)
          # Print Confirmation
          if self.verbose:
            print(f'BB indicators normalized')
            print(f'Normalized BB List : {self.norm_bb_list}')
            print('Len Normalized BB List :',len(self.norm_bb_list))

        case _:
          print(f'Indicator {indicator} not identified nor normalized')
        
  def apply_indicators(self):
    for indicator in self.indicators_list:

      # Case as per indicators
      match indicator:

        # Moving Volume
        case 'VMA':
          self.vma_periods = [5,10,20,50,100,200]
          for period in self.vma_periods:
            vma_name = f'VMA-{period}'
            self.df[vma_name] = self.df['Volume'].rolling(window=period).mean()
            self.vma_list.append(vma_name)
          if self.verbose:
            print(f'VMA indicators applied for periods {self.vma_periods}')
            print(f'VMA List : {self.vma_list}')
            print('Len VMA List :',len(self.vma_list))
        
        # Moving Averages
        case 'MA':
          self.ma_periods = [5,10,20,50,100,200]
          for period in self.ma_periods:
            ma_name = f'MA-{period}'
            self.df[ma_name] = self.df['Close'].rolling(window=period).mean()
            self.ma_list.append(ma_name)
          if self.verbose:
            print(f'MA indicators applied for periods {self.ma_periods}')
            print(f'MA List : {self.ma_list}')
            print('Len MA List :',len(self.ma_list))

        # Exponential Moving Averages
        case 'EMA':
          self.ema_periods = [9,12,21,26,50,100,200]
          for period in self.ema_periods:
            ema_name = f'EMA-{period}'
            self.df[ema_name] = self.df['Close'].ewm(span=period, adjust=False).mean()
            self.ema_list.append(ema_name)
          if self.verbose:
            print(f'EMA indicators applied for periods {self.ema_periods}')
            print(f'EMA List : {self.ema_list}')
            print('Len EMA List :',len(self.ema_list))

        # Relative Strength Index
        case 'RSI':
          self.rsi_periods = [9,10,14,20,25]
          #print(self.rsi_periods)
          for period in self.rsi_periods:
            rsi_name = f'RSI-{period}'
            self.df[rsi_name] = self.compute_rsi(window=period)
            self.rsi_list.append(rsi_name)
          if self.verbose:
            print(f'RSI indicators applied for periods {self.rsi_periods}')
            print(f'RSI List : {self.rsi_list}')
            print('Len RSI List :',len(self.rsi_list))

        # True Range (TR)
        case 'TR':
          self.compute_TR()
          self.tr_list = ['TR']
          if self.verbose:
            print('TR indicator applied')

        # Average True Range (ATR)
        case 'ATR':
          self.atr_periods = [7,10,14,20,30]
          if 'TR' not in self.df.columns:
            self.compute_TR()
          # Calculating Relative Strength Index (RSI) for different periods
          for period in self.atr_periods:
            atr_name = f'ATR-{period}'
            self.df[atr_name] = self.df['TR'].rolling(period).mean()
            self.atr_list.append(atr_name)
          if self.verbose:
            print(f'ATR indicators applied for periods {self.atr_periods}')
            print(f'ATR List : {self.atr_list}')
            print('Len ATR List :',len(self.atr_list))

        # Volume Weighted Average Price (VWAP)
        case 'VWAP':
            self.vwap_list=['VWAP']
            if 'VWAP' not in self.df.columns:
                self.df['VWAP']=ta.vwap(self.df.High, self.df.Low, self.df.Close, self.df.Volume)
                if self.verbose:
                    print('VWAP indicator applied')
                    print(f'VWAP List : {self.vwap_list}')
                    print('Len VWAP List :',len(self.vwap_list))

        # Bollinger Bands
        case 'BB':
            self.bb_periods = [10,14,20,50]
            self.bb_std_dev = [1.0,2.0,3.0]
            for period in self.bb_periods:
                for stddev in self.bb_std_dev:
                    #bb_name = f'BB-{period}-{stddev}'
                    bbands = [0]*len(self.df)
                    bbands = ta.bbands(self.df['Close'], length=period, std=stddev)
                    # Set the index of bbands to match the index of self.df
                    bbands.index = self.df.index
                    self.df = self.df.join(bbands)
                    #self.df = self.df.join(bbands, on='index')
                    self.bb_list.append(bbands.columns)
                    
            if self.verbose:
                print(f'BB indicator applied for periods {self.bb_periods} and std dev {self.bb_std_dev}')
                print(f'BB List : {self.bb_list}')
                print('Len BB List :',len(self.bb_list))

                
        case _:
          print(f'Indicator {indicator} not identified nor implemented')

  def apply_trend_signals(self):
    for trend_signal in self.trend_signals_list:

      # Case as per indicators
      match trend_signal:

        case 'SoloEMA':
          # Calculating EMA Signals for different periods and backcandles
          backcandles_count = [1,2,3,4,5]
          for ema in self.ema_list:
            for backcandles in backcandles_count:
              solo_ema_Trendsignal_name = 'TS-SoloEMA_'+ema +'_bc-'+str(backcandles)
              self.df[solo_ema_Trendsignal_name] = self.compute_SoloEMA_TrendSignal(ema,backcandles)
              self.TS_list.append(solo_ema_Trendsignal_name)
              self.SoloEMA_TrendSignal_list.append(solo_ema_Trendsignal_name)
          if self.verbose:
            print(f'SoloEMA Trend Signals applied for EMA {ema} and backcandles {backcandles}')
            print(f'SoloEMA List : {self.SoloEMA_TrendSignal_list}')
            print('Len SoloEMA List :',len(self.SoloEMA_TrendSignal_list))
            print('SoloEMA Trend Signalsadded to TS_list')

        case 'EMACrossover':
            self.ema_periods = [9,12,21,26,50,100,200]
            new_columns = {}  # Dictionary to hold new columns
            for long_var in range(len(self.ema_periods)):
                long_ema_period = self.ema_periods[long_var]
                for short_ema_period in self.ema_periods[:long_var]:
                    long_ema = f'EMA-{long_ema_period}'
                    short_ema = f'EMA-{short_ema_period}'
                    EMACrossover = f'TS-EMACrossover-{long_ema}-{short_ema}'
                    '''
                    # Generate signals
                    self.df[EMACrossover] = 0
                    #print("!!!!! UPTREND is 2 !!!!") 
                    #print("!!!!! DOWNTREND is 1 !!!!")
                    self.df[EMACrossover] = np.where(self.df[short_ema] > self.df[long_ema], 1, 2)
                    # Position 
                    self.df[f'positions_{EMACrossover}'] = self.df[EMACrossover].diff().copy(deep=True)
                    '''
                    # Generate signals as a pandas Series
                    EMACrossover_series = pd.Series(
                        np.where(self.df[short_ema] > self.df[long_ema], 1, 2),
                        index=self.df.index,
                        name=EMACrossover
                    )
                    new_columns[EMACrossover] = EMACrossover_series
        
                    # Calculate positions using pandas diff()
                    positions_EMACrossover_series = EMACrossover_series.diff().fillna(0)
                    positions_EMACrossover_series.name = f'positions_{EMACrossover}'
                    new_columns[f'positions_{EMACrossover}'] = positions_EMACrossover_series
                    
                    self.TS_list.append(EMACrossover)
                    self.EMACrossover_TrendSignal_list.append(EMACrossover)
                if self.verbose:
                    print(f'EMACrossover Trend Signals applied for EMA periods {self.ema_periods}')
                    print(f'EMACrossover List : {self.EMACrossover_TrendSignal_list}')
                    print('Len SoloEMA List :',len(self.EMACrossover_TrendSignal_list))
                    print('EMACrossover Trend Signals added to TS_list')

            # After the loops, create a DataFrame from new_columns
            new_columns_df = pd.DataFrame(new_columns, index=self.df.index)
            # Concatenate the new columns to self.df
            self.df = pd.concat([self.df, new_columns_df], axis=1)

        
        case _:
          print(f'Trend Signal {trend_signal} not identified nor implemented')


  
    
  def apply_reversal_signals(self):
    # --- RSI-Based Reversal Signals ---
    for period in self.rsi_periods:
        rsi_col = f'RSI-{period}'
        if rsi_col not in self.df.columns:
            self.df[rsi_col] = self.compute_rsi(window=period)
            self.rsi_list.append(rsi_col)

        for thresh in self.rsi_sensitivity:
            rs_name = f'RSIReversal_RSI-{period}_Thresh{thresh}'
            if rs_name not in self.df.columns:
                self.df[rs_name] = np.where(
                    self.df[rsi_col] < thresh, 2,  # Buy
                    np.where(self.df[rsi_col] > (100 - thresh), 1, 0)  # Sell / Neutral
                )
            self.rsi_reversal_list.append(rs_name)
            self.RS_list.append(rs_name)

    # --- BB-Based Reversal Signals ---
    bb_periods = [10, 20, 50]
    bb_std_devs = [1.0, 2.0, 3.0]
    bb_sensitivities = [0.95, 1.0, 1.05]

    for period in bb_periods:
        for std in bb_std_devs:
            bbands = ta.bbands(self.df['Close'], length=period, std=std)
            bbands.index = self.df.index

            # Column names
            lower, mid, upper = bbands.columns[0], bbands.columns[1], bbands.columns[2]

            # Add only if not already there
            for col in bbands.columns:
                if col not in self.df.columns:
                    self.df[col] = bbands[col]

            for sens in bb_sensitivities:
                rs_name = f'BBReversal_P{period}_SD{int(std*10)}_Sens{int(sens*100)}'
                if rs_name not in self.df.columns:
                    self.df[rs_name] = np.where(
                        self.df['Close'] < self.df[lower] * sens, 2,
                        np.where(self.df['Close'] > self.df[upper] * sens, 1, 0)
                    )
                self.bb_reversal_list.append(rs_name)
                self.RS_list.append(rs_name)

    # --- Price Action Reversal Signals ---
    for n in [1, 2, 3, 5]:
        rs_name = f'PriceReversal_N{n}'
        if rs_name not in self.df.columns:
            signal = np.zeros(len(self.df))
            signal = np.where(self.df['Close'] > self.df['Close'].shift(n), 1, signal)
            signal = np.where(self.df['Close'] < self.df['Close'].shift(n), 2, signal)
            self.df[rs_name] = signal
        self.price_action_reversal_list.append(rs_name)
        self.RS_list.append(rs_name)

    # Store all into one master list
    self.reversal_indicators_list.extend([
        self.rsi_reversal_list,
        self.bb_reversal_list,
        self.price_action_reversal_list
    ])

    if self.verbose:
        print('===== Reversal Signals Generated =====')
        print(f'Total RSI Reversals: {len(self.rsi_reversal_list)}')
        print(f'Total BB Reversals: {len(self.bb_reversal_list)}')
        print(f'Total Price Action Reversals: {len(self.price_action_reversal_list)}')
        print(f'Total RS Signals: {len(self.RS_list)}')


    
  # TotSignal column
  '''
  def compute_Tot_signal(self,EMAsignal,RSI,rsi_sens):
    totsignal = [0]*len(self.df)
    conditions = [
                  (self.df[EMAsignal]==1) & (self.df[RSI] >= 100 - rsi_sens),  # Condition 1 FOR Sell
                  (self.df[EMAsignal]==2) & (self.df[RSI] <= rsi_sens)  # Condition 2 FOR Buy
                ]
    choices = [1,2]  # Choice 1 for Condition 1 and Choice 2 for Condition 2

    # Use np.select to apply conditions and choices
    totsignal = np.select(conditions, choices, default=0)  # default is used where no condition is True
    count_longs = (totsignal == 2).sum()
    count_shorts = (totsignal == 1).sum()
    count_trades = count_longs + count_shorts
    return totsignal , count_trades , count_longs , count_shorts
  '''
  def compute_Tot_signal(self, signal_name, rsi_name, rsi_sens):
    totsignal = np.zeros(len(self.df), dtype=int)

    # Determine signal type
    if signal_name.startswith("RSIReversal") or signal_name.startswith("BBReversal") or signal_name.startswith("PriceReversal"):
        # Reversal-based logic
        totsignal = np.where((self.df[signal_name] == 2) & (self.df[rsi_name] <= rsi_sens), 2, totsignal)  # Buy
        totsignal = np.where((self.df[signal_name] == 1) & (self.df[rsi_name] >= 100 - rsi_sens), 1, totsignal)  # Sell

    elif signal_name.startswith("TS-SoloEMA") or signal_name.startswith("TS-EMACrossover"):
        # Trend-following logic
        totsignal = np.where((self.df[signal_name] == 2) & (self.df[rsi_name] <= rsi_sens), 2, totsignal)  # Buy
        totsignal = np.where((self.df[signal_name] == 1) & (self.df[rsi_name] >= 100 - rsi_sens), 1, totsignal)  # Sell

    else:
        # Default fallback logic (could also raise a warning)
        totsignal = np.where((self.df[rsi_name] <= rsi_sens), 2, totsignal)
        totsignal = np.where((self.df[rsi_name] >= 100 - rsi_sens), 1, totsignal)

    count_longs = (totsignal == 2).sum()
    count_shorts = (totsignal == 1).sum()
    count_trades = count_longs + count_shorts

    return totsignal, count_trades, count_longs, count_shorts

  

  '''
  def apply_Tot_signals(self):
    Explored_Signals_Count = 0
    Useful_Signals_Count = 0
    rsi_sensitivity = [5,10,15]
    for trend_signal in self.TS_list:
      for rsi in self.rsi_list:
        for rsi_sens in rsi_sensitivity:
          totsignal_name = 'TotSignal_-'+rsi+'_RSISens-'+str(rsi_sens)+'_'+trend_signal
          # Ignore PerformanceWarning
          with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=pd.errors.PerformanceWarning)
            self.df[totsignal_name] , Count_Trades , Count_Longs , Count_Shorts = self.compute_Tot_signal(trend_signal,rsi,rsi_sens)
          Explored_Signals_Count = Explored_Signals_Count + 1
          if Count_Trades > 0:
            self.TotSignal_list.append(totsignal_name)
            self.TotSignal_Trade_Counts.append(Count_Trades)
            self.TotSignal_Long_Counts.append(Count_Longs)
            self.TotSignal_Short_Counts.append(Count_Shorts)
            Useful_Signals_Count = Useful_Signals_Count + 1
              
    if self.verbose:
      print('Calculated Signals Count:',Explored_Signals_Count)
      print('Useful Signals Count:',Useful_Signals_Count)
      print('TotSignal List :',self.TotSignal_list)
  '''
  def apply_Tot_signals(self, include_reversal=True):
    Explored_Signals_Count = 0
    Useful_Signals_Count = 0

    # Use TS_list + RS_list if reversal is included
    signal_pool = self.TS_list + self.RS_list if include_reversal else self.TS_list

    for signal in signal_pool:
        for rsi in self.rsi_list:
            for rsi_sens in self.rsi_sensitivity:
                totsignal_name = f'TotSignal_-{rsi}_RSISens-{rsi_sens}_{signal}'

                if totsignal_name in self.df.columns:
                    continue  # Avoid recomputation

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=pd.errors.PerformanceWarning)
                    self.df[totsignal_name], Count_Trades, Count_Longs, Count_Shorts = self.compute_Tot_signal(signal, rsi, rsi_sens)

                Explored_Signals_Count += 1
                if Count_Trades > 0:
                    self.TotSignal_list.append(totsignal_name)
                    self.TotSignal_Trade_Counts.append(Count_Trades)
                    self.TotSignal_Long_Counts.append(Count_Longs)
                    self.TotSignal_Short_Counts.append(Count_Shorts)
                    Useful_Signals_Count += 1

    if self.verbose:
        print("===== TotSignal Generation Complete =====")
        print("Total Signals Explored:", Explored_Signals_Count)
        print("Useful Signals Found:", Useful_Signals_Count)
        print("Top 5 TotSignals:", self.TotSignal_list[:5])



    
  def get_explored_signals_df(self):
      # Create a DataFrame
      self.explored_signals_df = pd.DataFrame({
                      'Signal Name': self.TotSignal_list,
                      'Trades': self.TotSignal_Trade_Counts,
                      'Longs': self.TotSignal_Long_Counts,
                      'Shorts': self.TotSignal_Short_Counts
                      })
      if self.verbose:
        print('Explored Trade Signals info and Statistics')
        print(self.explored_signals_df)

"""


# In[6]:


class Feat_Eng:
    def __init__(self, dir_path=None, df=None, indicators_list=[], trend_signals_list=[], TA_indicators_parameters=None, verbose=False):
        # --- Default Indicator Lists ---
        self.vma_list = []
        self.ma_list = []
        self.ema_list = []
        self.rsi_list = []
        self.tr_list = []
        self.atr_list = []
        self.vwap_list = []
        self.bb_list = []

        # --- Normalized Lists ---
        self.norm_indicators_list = []
        self.norm_vma_list = []
        self.norm_ma_list = []
        self.norm_ema_list = []
        self.norm_rsi_list = []
        self.norm_tr_list = []
        self.norm_atr_list = []
        self.norm_vwap_list = []
        self.norm_bb_list = []

        # --- Trend Signals ---
        self.TS_list = []
        self.SoloEMA_TrendSignal_list = []
        self.EMACrossover_TrendSignal_list = []
        self.TS_Slope_list = []

        # --- Reversal Signals ---
        self.RS_list = []
        self.reversal_indicators_list = []
        self.rsi_reversal_list = []
        self.bb_reversal_list = []
        self.price_action_reversal_list = []
        self.atr_spike_reversal_list = []
        self.engulfing_pattern_list = []

        # --- Other Strategies ---
        self.vol_breakout_list = []
        self.inside_bar_list = []
        self.donchian_channel_list = []
        self.zscore_reversion_list = []
        self.rangebound_filter_list = []

        # --- Tot Signals ---
        self.TotSignal_list = []
        self.TotSignal_Trade_Counts = []
        self.TotSignal_Long_Counts = []
        self.TotSignal_Short_Counts = []

        # --- Input & Config ---
        self.dir_path = dir_path
        self.df = df
        self.indicators_list = indicators_list
        self.trend_signals_list = trend_signals_list
        self.verbose = verbose

        # --- Derived Paths ---
        if self.dir_path:
            self.Feat_Eng_file_path = os.path.join(self.dir_path, 'Feat_Eng.pkl')
        else:
            self.Feat_Eng_file_path = 'Feat_Eng.pkl'

        # --- Default Parameter Sets ---
        default_params = {
            "ema_periods": [9, 12, 21, 26, 50, 100, 200],
            "rsi_periods": [9, 10, 14, 20, 25],
            "atr_periods": [7, 10, 14, 20, 30],
            "backcandles_count": [1, 2, 3, 4, 5],
            "rsi_sensitivity": [5,10,15,17,20,22,25],
            "zscore_periods": [20, 50, 100],
            "donchian_lengths": [10, 20, 50],
            "atr_spike_mults": [1.5, 2.0],
            "inside_bar_lookbacks": [1, 2],
            "engulfing_lookbacks": [1]
        }
        self.TA_indicators_parameters = TA_indicators_parameters or default_params

        self.ema_periods = self.TA_indicators_parameters['ema_periods']
        self.rsi_periods = self.TA_indicators_parameters['rsi_periods']
        self.atr_periods = self.TA_indicators_parameters['atr_periods']
        self.backcandles_count = self.TA_indicators_parameters['backcandles_count']
        self.rsi_sensitivity = self.TA_indicators_parameters['rsi_sensitivity']

    def run(self, Indicators=True, Normalizations=True, Trend_Signals=True, Reversal_Signals=True, Tot_Signals=True,Rank = False, Save=True):
        self.rename_columns()

        if Indicators:
            if self.verbose: print("Calculating indicators...")
            self.apply_indicators()
            if Normalizations:
                if self.verbose: print("Normalizing indicators...")
                self.normalize_indicators()

        if Trend_Signals:
            if self.verbose: print("Computing trend signals...")
            self.apply_trend_signals()

        if Reversal_Signals:
            if self.verbose: print("Computing reversal signals...")
            self.apply_reversal_signals()

        if self.verbose: print("Computing custom strategies...")
        self.apply_vol_breakout()
        self.apply_zscore_reversion()
        self.apply_rangebound_filter()
        self.apply_atr_spike_reversal()
        self.apply_donchian_channel_cross()
        self.apply_inside_bar_breakout()
        self.apply_engulfing_pattern()
        self.apply_slope_trend()

        if self.verbose: print("Computing Advanced strategies...")
        self.apply_advanced_signals()

        if Tot_Signals:
            if self.verbose: print("Computing TotSignals...")
            self.apply_Tot_signals()
        if Rank:
            self.rank_TotSignal()
            self.get_explored_signals_df()
            self.AnalysisRank()

        if Save:
            self.save_data()
            self.save_to_file(self.Feat_Eng_file_path)




    def apply_advanced_signals(self):
        if self.verbose: print("Applying RSI-MACD Divergence...")
        self.apply_rsi_macd_divergence()

        if self.verbose: print("Applying Rate of Change Momentum Shift...")
        self.apply_rate_of_change()

        if self.verbose: print("Applying Keltner Channels...")
        self.apply_keltner_channels()

        if self.verbose: print("Applying Hurst Exponent...")
        self.apply_hurst_exponent()

        if self.verbose: print("Applying OBV Spike Detection...")
        self.apply_on_balance_volume()

        if self.verbose: print("Applying ADX Trend Strength...")
        self.apply_adx_strength()

        if self.verbose: print("Applying Open Range Breakout...")
        self.apply_open_range_breakout()

        if self.verbose: print("Applying Fractal Reversal Detection...")
        self.apply_fractal_reversal()

        if self.verbose: print("Applying Bar Strength Index...")
        self.apply_bar_strength_index()

        if self.verbose: print("Applying VWAP Intraday Bands...")
        self.apply_vwap_bands()

        if self.verbose: print("Applying Volatility Compression Breakout...")
        self.apply_volatility_compression()
        

    def apply_rsi_macd_divergence(self):
        rsi = ta.rsi(self.df['Close'], length=14)
        macd = ta.macd(self.df['Close'])['MACD_12_26_9']
        self.df['RSI_MACD_Divergence'] = np.where((rsi > 70) & (macd < 0), 1,
                                                  np.where((rsi < 30) & (macd > 0), 2, 0))
        self.RS_list.append('RSI_MACD_Divergence')

    def apply_rate_of_change(self):
        roc = ta.roc(self.df['Close'], length=10)
        self.df['ROC_Shift'] = np.where(roc > 5, 2, np.where(roc < -5, 1, 0))
        self.TS_list.append('ROC_Shift')

    def apply_keltner_channels(self):
        lengths = [10, 20, 30]
        scalars = [1.0, 1.5, 2.0]
        for length in lengths:
            for scalar in scalars:
                keltner = ta.kc(self.df['High'], self.df['Low'], self.df['Close'], length=length, scalar=scalar)
                if keltner is not None:
                    keltner.columns = [f'KCL_{length}_{scalar}', f'KC_{length}_{scalar}', f'KCU_{length}_{scalar}']
                    keltner.index = self.df.index
                    self.df = self.df.join(keltner)
                    name = f'VOL-KeltnerBreak_{length}_{scalar}'
                    self.df[name] = np.where(self.df['Close'] > self.df[f'KCU_{length}_{scalar}'], 2,
                                             np.where(self.df['Close'] < self.df[f'KCL_{length}_{scalar}'], 1, 0))
                    self.vol_breakout_list.append(name)

    def apply_hurst_exponent(self):
        def hurst(ts):
            lags = range(2, 20)
            tau = []
            for lag in lags:
                try:
                    tau_val = np.std(np.subtract(ts[lag:], ts[:-lag]))
                    if tau_val > 0:
                        tau.append(tau_val)
                    else:
                        tau.append(1e-6)
                except Exception:
                    tau.append(1e-6)
            log_lags = np.log(lags)
            log_tau = np.log(tau)
            poly = np.polyfit(log_lags, log_tau, 1)
            return poly[0]*2.0

        hurst_vals = [np.nan]*100
        for i in range(100, len(self.df)):
            close_slice = self.df['Close'].iloc[i-100:i]
            hurst_vals.append(hurst(close_slice))
        self.df['HurstExp'] = hurst_vals
        self.df['Hurst_Trend'] = np.where(self.df['HurstExp'] > 0.55, 2,
                                          np.where(self.df['HurstExp'] < 0.45, 1, 0))
        self.TS_list.append('Hurst_Trend')

    def apply_on_balance_volume(self):
        obv = ta.obv(self.df['Close'], self.df['Volume'])
        obv_slope = obv.diff(3)
        self.df['OBV_Spike'] = np.where(obv_slope > 0, 2, np.where(obv_slope < 0, 1, 0))
        self.TS_list.append('OBV_Spike')

    def apply_adx_strength(self):
        adx = ta.adx(self.df['High'], self.df['Low'], self.df['Close'], length=14)
        self.df['ADX_Trend'] = np.where(adx['ADX_14'] > 25, 2, 0)
        self.TS_list.append('ADX_Trend')

    def apply_open_range_breakout(self):
        self.df['ORB'] = 0
        if len(self.df) >= 30:
            open_range_high = self.df['High'].iloc[:30].max()
            open_range_low = self.df['Low'].iloc[:30].min()
            self.df['ORB'] = np.where(self.df['High'] > open_range_high, 2,
                                      np.where(self.df['Low'] < open_range_low, 1, 0))
        self.vol_breakout_list.append('ORB')

    def apply_fractal_reversal(self):
        self.df['Fractal_Rev'] = 0
        high = self.df['High']
        low = self.df['Low']
        
        for i in range(2, len(self.df) - 2):
            if (high.iloc[i] > high.iloc[i - 2] and
                high.iloc[i] > high.iloc[i - 1] and
                high.iloc[i] > high.iloc[i + 1] and
                high.iloc[i] > high.iloc[i + 2]):
                self.df.loc[self.df.index[i], 'Fractal_Rev'] = 1  # sell
    
            elif (low.iloc[i] < low.iloc[i - 2] and
                  low.iloc[i] < low.iloc[i - 1] and
                  low.iloc[i] < low.iloc[i + 1] and
                  low.iloc[i] < low.iloc[i + 2]):
                self.df.loc[self.df.index[i], 'Fractal_Rev'] = 2  # buy
    
        self.RS_list.append('Fractal_Rev')


    def apply_bar_strength_index(self):
        bar_strength = (self.df['Close'] - self.df['Open']) / (self.df['High'] - self.df['Low'] + 1e-6)
        self.df['BarStrength'] = np.where(bar_strength > 0.5, 2,
                                          np.where(bar_strength < -0.5, 1, 0))
        self.TS_list.append('BarStrength')

    def apply_vwap_bands(self):
        vwap = ta.vwap(self.df['High'], self.df['Low'], self.df['Close'], self.df['Volume'])
        std = self.df['Close'].rolling(20).std()
        self.df['VWAP'] = vwap
        self.df['VWAP_Upper'] = vwap + std
        self.df['VWAP_Lower'] = vwap - std
        self.df['VWAP_BandSignal'] = np.where(self.df['Close'] > self.df['VWAP_Upper'], 2,
                                              np.where(self.df['Close'] < self.df['VWAP_Lower'], 1, 0))
        self.vol_breakout_list.append('VWAP_BandSignal')

    def apply_volatility_compression(self):
        atr = ta.atr(self.df['High'], self.df['Low'], self.df['Close'], length=14)
        atr_ma = atr.rolling(20).mean()
        self.df['ATR_CompBreak'] = np.where((atr < atr_ma * 0.75) & (atr.shift(-1) > atr_ma), 1, 0)
        self.vol_breakout_list.append('ATR_CompBreak')
        
    def compute_solo_ema(self, ema_col, backcandles):
        signal = np.zeros(len(self.df), dtype=int)
        for i in range(backcandles, len(self.df)):
            highs = self.df['High'].iloc[i - backcandles:i + 1]
            lows = self.df['Low'].iloc[i - backcandles:i + 1]
            emas = self.df[ema_col].iloc[i - backcandles:i + 1]
            if all(lows > emas):
                signal[i] = 2
            elif all(highs < emas):
                signal[i] = 1
        return signal

    


    def compute_rsi(self, window=14):
        delta = self.df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def compute_TR(self):
        self.df['close_prev'] = self.df['Close'].shift(1)
        self.df['high-low'] = self.df['High'] - self.df['Low']
        self.df['high-close_prev'] = abs(self.df['High'] - self.df['close_prev'])
        self.df['low-close_prev'] = abs(self.df['Low'] - self.df['close_prev'])
        self.df['TR'] = self.df[['high-low', 'high-close_prev', 'low-close_prev']].max(axis=1)
    
    def compute_SoloEMA_TrendSignal(self, EMA_period , backcandles):
        emasignal = [0]*len(self.df)
        for row in range(backcandles-1, len(self.df)):
          upt = 1
          dnt = 1
          for i in range(row-backcandles, row+1):
              if self.df['High'].iloc[i] >= self.df[EMA_period].iloc[i]:
                  dnt = 0
              if self.df['Low'].iloc[i] <= self.df[EMA_period].iloc[i]:
                  upt = 0
          if upt==1 and dnt==1:
              #print("!!!!! check trend loop !!!!")
              emasignal[row]=3
          elif upt==1:
              #print("!!!!! UPTREND !!!!")
              emasignal[row]=2
          elif dnt==1:
              #print("!!!!! DOWNTREND !!!!")
              emasignal[row]=1
          else:
              #print("!!!!! Trend Reversal !!!!")
              emasignal[row]=0
        return emasignal
          
    def apply_indicators(self):
        for indicator in self.indicators_list:
    
          # Case as per indicators
          match indicator:
    
            # Moving Volume
            case 'VMA':
              self.vma_periods = [5,10,20,50,100,200]
              for period in self.vma_periods:
                vma_name = f'VMA-{period}'
                self.df[vma_name] = self.df['Volume'].rolling(window=period).mean()
                self.vma_list.append(vma_name)
              if self.verbose:
                print(f'VMA indicators applied for periods {self.vma_periods}')
                print(f'VMA List : {self.vma_list}')
                print('Len VMA List :',len(self.vma_list))
            
            # Moving Averages
            case 'MA':
              self.ma_periods = [5,10,20,50,100,200]
              for period in self.ma_periods:
                ma_name = f'MA-{period}'
                self.df[ma_name] = self.df['Close'].rolling(window=period).mean()
                self.ma_list.append(ma_name)
              if self.verbose:
                print(f'MA indicators applied for periods {self.ma_periods}')
                print(f'MA List : {self.ma_list}')
                print('Len MA List :',len(self.ma_list))
    
            # Exponential Moving Averages
            case 'EMA':
              self.ema_periods = [9,12,21,26,50,100,200]
              for period in self.ema_periods:
                ema_name = f'EMA-{period}'
                self.df[ema_name] = self.df['Close'].ewm(span=period, adjust=False).mean()
                self.ema_list.append(ema_name)
              if self.verbose:
                print(f'EMA indicators applied for periods {self.ema_periods}')
                print(f'EMA List : {self.ema_list}')
                print('Len EMA List :',len(self.ema_list))
    
            # Relative Strength Index
            case 'RSI':
              self.rsi_periods = [9,10,14,20,25]
              #print(self.rsi_periods)
              for period in self.rsi_periods:
                rsi_name = f'RSI-{period}'
                self.df[rsi_name] = self.compute_rsi(window=period)
                self.rsi_list.append(rsi_name)
              if self.verbose:
                print(f'RSI indicators applied for periods {self.rsi_periods}')
                print(f'RSI List : {self.rsi_list}')
                print('Len RSI List :',len(self.rsi_list))
    
            # True Range (TR)
            case 'TR':
              self.compute_TR()
              self.tr_list = ['TR']
              if self.verbose:
                print('TR indicator applied')
    
            # Average True Range (ATR)
            case 'ATR':
              self.atr_periods = [7,10,14,20,30]
              if 'TR' not in self.df.columns:
                self.compute_TR()
              # Calculating Relative Strength Index (RSI) for different periods
              for period in self.atr_periods:
                atr_name = f'ATR-{period}'
                self.df[atr_name] = self.df['TR'].rolling(period).mean()
                self.atr_list.append(atr_name)
              if self.verbose:
                print(f'ATR indicators applied for periods {self.atr_periods}')
                print(f'ATR List : {self.atr_list}')
                print('Len ATR List :',len(self.atr_list))
    
            # Volume Weighted Average Price (VWAP)
            case 'VWAP':
                self.vwap_list=['VWAP']
                if 'VWAP' not in self.df.columns:
                    self.df['VWAP']=ta.vwap(self.df.High, self.df.Low, self.df.Close, self.df.Volume)
                    if self.verbose:
                        print('VWAP indicator applied')
                        print(f'VWAP List : {self.vwap_list}')
                        print('Len VWAP List :',len(self.vwap_list))
    
            # Bollinger Bands
            case 'BB':
                self.bb_periods = [10,14,20,50]
                self.bb_std_dev = [1.0,2.0,3.0]
                for period in self.bb_periods:
                    for stddev in self.bb_std_dev:
                        #bb_name = f'BB-{period}-{stddev}'
                        bbands = [0]*len(self.df)
                        bbands = ta.bbands(self.df['Close'], length=period, std=stddev)
                        # Set the index of bbands to match the index of self.df
                        bbands.index = self.df.index
                        self.df = self.df.join(bbands)
                        #self.df = self.df.join(bbands, on='index')
                        self.bb_list.append(bbands.columns)
                        
                if self.verbose:
                    print(f'BB indicator applied for periods {self.bb_periods} and std dev {self.bb_std_dev}')
                    print(f'BB List : {self.bb_list}')
                    print('Len BB List :',len(self.bb_list))
    
                    
            case _:
              print(f'Indicator {indicator} not identified nor implemented')

    def apply_trend_signals(self):
        for trend_signal in self.trend_signals_list:
    
          # Case as per indicators
          match trend_signal:
    
            case 'SoloEMA':
              # Calculating EMA Signals for different periods and backcandles
              backcandles_count = [1,2,3,4,5]
              for ema in self.ema_list:
                for backcandles in backcandles_count:
                  solo_ema_Trendsignal_name = 'TS-SoloEMA_'+ema +'_bc-'+str(backcandles)
                  self.df[solo_ema_Trendsignal_name] = self.compute_SoloEMA_TrendSignal(ema,backcandles)
                  self.TS_list.append(solo_ema_Trendsignal_name)
                  self.SoloEMA_TrendSignal_list.append(solo_ema_Trendsignal_name)
              if self.verbose:
                print(f'SoloEMA Trend Signals applied for EMA {ema} and backcandles {backcandles}')
                print(f'SoloEMA List : {self.SoloEMA_TrendSignal_list}')
                print('Len SoloEMA List :',len(self.SoloEMA_TrendSignal_list))
                print('SoloEMA Trend Signalsadded to TS_list')
    
            case 'EMACrossover':
                self.ema_periods = [9,12,21,26,50,100,200]
                new_columns = {}  # Dictionary to hold new columns
                for long_var in range(len(self.ema_periods)):
                    long_ema_period = self.ema_periods[long_var]
                    for short_ema_period in self.ema_periods[:long_var]:
                        long_ema = f'EMA-{long_ema_period}'
                        short_ema = f'EMA-{short_ema_period}'
                        EMACrossover = f'TS-EMACrossover-{long_ema}-{short_ema}'
                        '''
                        # Generate signals
                        self.df[EMACrossover] = 0
                        #print("!!!!! UPTREND is 2 !!!!") 
                        #print("!!!!! DOWNTREND is 1 !!!!")
                        self.df[EMACrossover] = np.where(self.df[short_ema] > self.df[long_ema], 1, 2)
                        # Position 
                        self.df[f'positions_{EMACrossover}'] = self.df[EMACrossover].diff().copy(deep=True)
                        '''
                        # Generate signals as a pandas Series
                        EMACrossover_series = pd.Series(
                            np.where(self.df[short_ema] > self.df[long_ema], 1, 2),
                            index=self.df.index,
                            name=EMACrossover
                        )
                        new_columns[EMACrossover] = EMACrossover_series
            
                        # Calculate positions using pandas diff()
                        positions_EMACrossover_series = EMACrossover_series.diff().fillna(0)
                        positions_EMACrossover_series.name = f'positions_{EMACrossover}'
                        new_columns[f'positions_{EMACrossover}'] = positions_EMACrossover_series
                        
                        self.TS_list.append(EMACrossover)
                        self.EMACrossover_TrendSignal_list.append(EMACrossover)
                    if self.verbose:
                        print(f'EMACrossover Trend Signals applied for EMA periods {self.ema_periods}')
                        print(f'EMACrossover List : {self.EMACrossover_TrendSignal_list}')
                        print('Len SoloEMA List :',len(self.EMACrossover_TrendSignal_list))
                        print('EMACrossover Trend Signals added to TS_list')
    
                # After the loops, create a DataFrame from new_columns
                new_columns_df = pd.DataFrame(new_columns, index=self.df.index)
                # Concatenate the new columns to self.df
                self.df = pd.concat([self.df, new_columns_df], axis=1)
    
            
            case _:
              print(f'Trend Signal {trend_signal} not identified nor implemented')
    
    
      
        
    def apply_reversal_signals(self):
        # --- RSI-Based Reversal Signals ---
        for period in self.rsi_periods:
            rsi_col = f'RSI-{period}'
            if rsi_col not in self.df.columns:
                self.df[rsi_col] = self.compute_rsi(window=period)
                self.rsi_list.append(rsi_col)
    
            for thresh in self.rsi_sensitivity:
                rs_name = f'RSIReversal_RSI-{period}_Thresh{thresh}'
                if rs_name not in self.df.columns:
                    self.df[rs_name] = np.where(
                        self.df[rsi_col] < thresh, 2,  # Buy
                        np.where(self.df[rsi_col] > (100 - thresh), 1, 0)  # Sell / Neutral
                    )
                self.rsi_reversal_list.append(rs_name)
                self.RS_list.append(rs_name)
    
        # --- BB-Based Reversal Signals ---
        bb_periods = [10, 20, 50]
        bb_std_devs = [1.0, 2.0, 3.0]
        bb_sensitivities = [0.95, 1.0, 1.05]
    
        for period in bb_periods:
            for std in bb_std_devs:
                bbands = ta.bbands(self.df['Close'], length=period, std=std)
                bbands.index = self.df.index
    
                # Column names
                lower, mid, upper = bbands.columns[0], bbands.columns[1], bbands.columns[2]
    
                # Add only if not already there
                for col in bbands.columns:
                    if col not in self.df.columns:
                        self.df[col] = bbands[col]
    
                for sens in bb_sensitivities:
                    rs_name = f'BBReversal_P{period}_SD{int(std*10)}_Sens{int(sens*100)}'
                    if rs_name not in self.df.columns:
                        self.df[rs_name] = np.where(
                            self.df['Close'] < self.df[lower] * sens, 2,
                            np.where(self.df['Close'] > self.df[upper] * sens, 1, 0)
                        )
                    self.bb_reversal_list.append(rs_name)
                    self.RS_list.append(rs_name)
    
        # --- Price Action Reversal Signals ---
        for n in [1, 2, 3, 5]:
            rs_name = f'PriceReversal_N{n}'
            if rs_name not in self.df.columns:
                signal = np.zeros(len(self.df))
                signal = np.where(self.df['Close'] > self.df['Close'].shift(n), 1, signal)
                signal = np.where(self.df['Close'] < self.df['Close'].shift(n), 2, signal)
                self.df[rs_name] = signal
            self.price_action_reversal_list.append(rs_name)
            self.RS_list.append(rs_name)
    
        # Store all into one master list
        self.reversal_indicators_list.extend([
            self.rsi_reversal_list,
            self.bb_reversal_list,
            self.price_action_reversal_list
        ])
    
        if self.verbose:
            print('===== Reversal Signals Generated =====')
            print(f'Total RSI Reversals: {len(self.rsi_reversal_list)}')
            print(f'Total BB Reversals: {len(self.bb_reversal_list)}')
            print(f'Total Price Action Reversals: {len(self.price_action_reversal_list)}')
            print(f'Total RS Signals: {len(self.RS_list)}')
      
    
    def apply_additional_reversals(self):
        self.apply_vol_breakout()
        self.apply_atr_spike()
        self.apply_engulfing_pattern()
        self.apply_inside_bar()
        self.apply_donchian()
        self.apply_zscore_reversion()
        self.apply_slope_trend()
        self.apply_range_filter()
        
    def rename_columns(self):
        mapping = {
            'open': 'Open', 'high': 'High', 'low': 'Low',
            'close': 'Close', 'volume': 'Volume',
            'vwap': 'VWAP', 'trade_count': 'Trade Count'
        }
        self.df.rename(columns={k: v for k, v in mapping.items() if k in self.df.columns}, inplace=True)

    def apply_vol_breakout(self):
        for window in [10, 20, 50]:
            name = f'VOL-Breakout_{window}'
            rolling_range = (self.df['High'] - self.df['Low']).rolling(window=window)
            breakout = (self.df['High'] - self.df['Low']) > rolling_range.mean() + rolling_range.std()
            self.df[name] = np.where(breakout, 1, 0)
            self.vol_breakout_list.append(name)

    def apply_atr_spike_reversal(self):
        for period in self.atr_periods:
            name = f'RS-ATRSpike_ATR{period}'
            if f'ATR-{period}' not in self.df.columns:
                self.df[f'ATR-{period}'] = ta.atr(self.df['High'], self.df['Low'], self.df['Close'], length=period)
            atr = self.df[f'ATR-{period}']
            threshold = atr.mean() + atr.std()
            self.df[name] = np.where(atr > threshold, 1, 0)
            self.atr_spike_reversal_list.append(name)
            self.RS_list.append(name)

    def apply_engulfing_pattern(self):
        name = "PAT-Engulfing"
        engulfs = np.zeros(len(self.df))
        prev_open = self.df['Open'].shift(1)
        prev_close = self.df['Close'].shift(1)
        curr_open = self.df['Open']
        curr_close = self.df['Close']
        bull = (prev_close < prev_open) & (curr_close > curr_open) & (curr_close > prev_open) & (curr_open < prev_close)
        bear = (prev_close > prev_open) & (curr_close < curr_open) & (curr_close < prev_open) & (curr_open > prev_close)
        engulfs[bull] = 2
        engulfs[bear] = 1
        self.df[name] = engulfs
        self.engulfing_pattern_list.append(name)
        self.RS_list.append(name)

    def apply_inside_bar_breakout(self):
        for n in [1, 2, 3]:
            name = f'PAT-InsideBar_{n}'
            inside = (self.df['High'] < self.df['High'].shift(n)) & (self.df['Low'] > self.df['Low'].shift(n))
            breakout_up = (self.df['High'] > self.df['High'].shift(n)) & inside
            breakout_down = (self.df['Low'] < self.df['Low'].shift(n)) & inside
            signal = np.where(breakout_up, 2, np.where(breakout_down, 1, 0))
            self.df[name] = signal
            self.inside_bar_list.append(name)
            self.RS_list.append(name)

    def apply_donchian_channel_cross(self):
        for period in [20, 50, 100]:
            name = f'VOL-Donchian_{period}'
            highest = self.df['High'].rolling(window=period).max()
            lowest = self.df['Low'].rolling(window=period).min()
            breakout_up = self.df['Close'] > highest.shift(1)
            breakout_down = self.df['Close'] < lowest.shift(1)
            signal = np.where(breakout_up, 2, np.where(breakout_down, 1, 0))
            self.df[name] = signal
            self.donchian_channel_list.append(name)
            self.TS_list.append(name)

    def apply_zscore_reversion(self):
        for period in [20, 50, 100]:
            name = f'REV-ZScore_{period}'
            mean = self.df['Close'].rolling(window=period).mean()
            std = self.df['Close'].rolling(window=period).std()
            zscore = (self.df['Close'] - mean) / std
            self.df[name] = zscore
            self.zscore_reversion_list.append(name)

    def apply_slope_trend(self):
        name = 'TS-Slope'
        slope = self.df['Close'].diff(3)
        self.df[name] = np.where(slope > 0, 2, np.where(slope < 0, 1, 0))
        self.TS_list.append(name)

    def apply_rangebound_filter(self):
        for window in [10, 20, 50]:
            name = f'FLTR-RangeBound_{window}'
            range_val = self.df['High'].rolling(window).max() - self.df['Low'].rolling(window).min()
            self.df[name] = range_val / self.df['Close']
            self.rangebound_filter_list.append(name)

    def save_to_file(self, filename):
        with open(filename, 'wb') as file:
            pickle.dump(self, file)
        if self.verbose:
            print(f"Feat_Eng instance saved to {filename}")


    def compute_Tot_signal(self, signal_name, rsi_name, rsi_sens):
        totsignal = np.zeros(len(self.df), dtype=int)
        if signal_name in self.df.columns:
            if signal_name.startswith("TS"):
                totsignal = np.where((self.df[signal_name] == 2) & (self.df[rsi_name] <= rsi_sens), 2, totsignal)
                totsignal = np.where((self.df[signal_name] == 1) & (self.df[rsi_name] >= 100 - rsi_sens), 1, totsignal)
            else:
                totsignal = np.where((self.df[signal_name] == 2) & (self.df[rsi_name] <= rsi_sens), 2, totsignal)
                totsignal = np.where((self.df[signal_name] == 1) & (self.df[rsi_name] >= 100 - rsi_sens), 1, totsignal)
        count_longs = (totsignal == 2).sum()
        count_shorts = (totsignal == 1).sum()
        count_trades = count_longs + count_shorts
        return totsignal, count_trades, count_longs, count_shorts

    def apply_Tot_signals(self):
        for signal in self.TS_list + self.RS_list:
            for rsi in self.rsi_list:
                for sens in self.rsi_sensitivity:
                    name = f'TotSignal_RSI-{rsi}_RSISens-{sens}_{signal}'
                    self.df[name], trades, longs, shorts = self.compute_Tot_signal(signal, rsi, sens)
                    if trades > 0:
                        self.TotSignal_list.append(name)
                        self.TotSignal_Trade_Counts.append(trades)
                        self.TotSignal_Long_Counts.append(longs)
                        self.TotSignal_Short_Counts.append(shorts)

    def rank_TotSignal(self):
        """
        Sort and rank all signals in TotSignal_list by number of trades (descending).
        Reorder the TotSignal_Trade_Counts, TotSignal_Long_Counts, and TotSignal_Short_Counts lists accordingly.
        """
        # 1. Combine signals with their counts into tuples for synchronized sorting
        combined = list(zip(self.TotSignal_Trade_Counts, 
                             self.TotSignal_Long_Counts, 
                             self.TotSignal_Short_Counts, 
                             self.TotSignal_list))
        
        # 2. Sort the combined list by trade count (index 0 of tuple) in descending order
        combined.sort(key=lambda x: x[0], reverse=True)
        
        # 3. Unzip the sorted tuples back into individual lists (convert tuples to lists for mutability)
        self.TotSignal_Trade_Counts, self.TotSignal_Long_Counts, self.TotSignal_Short_Counts, self.TotSignal_list = map(list, zip(*combined))


    def get_explored_signals_df(self):
      # Create a DataFrame
      self.explored_signals_df = pd.DataFrame({
                      'Signal Name': self.TotSignal_list,
                      'Trades': self.TotSignal_Trade_Counts,
                      'Longs': self.TotSignal_Long_Counts,
                      'Shorts': self.TotSignal_Short_Counts
                      })
      if self.verbose:
        print('Explored Trade Signals info and Statistics')
        print(self.explored_signals_df)

    def AnalysisRank(self):
        peaks = argrelextrema(self.df['Close'].values, np.greater)[0]
        troughs = argrelextrema(self.df['Close'].values, np.less)[0]

        ranks = []

        for sig in self.TotSignal_list:
            if sig not in self.df.columns:
                continue

            signal_vals = self.df[sig].values
            buy_indices = np.where(signal_vals == 2)[0]
            sell_indices = np.where(signal_vals == 1)[0]
            total_trades = len(buy_indices) + len(sell_indices)

            # 1. Profit-Based Metrics
            profits = []
            win_count = 0
            holding_period = 5
            for idx in buy_indices:
                if idx + holding_period < len(self.df):
                    entry = self.df['Close'].iloc[idx]
                    exit = self.df['Close'].iloc[idx + holding_period]
                    ret = (exit - entry) / entry
                    profits.append(ret)
                    if ret > 0: win_count += 1
            for idx in sell_indices:
                if idx + holding_period < len(self.df):
                    entry = self.df['Close'].iloc[idx]
                    exit = self.df['Close'].iloc[idx + holding_period]
                    ret = (entry - exit) / entry
                    profits.append(ret)
                    if ret > 0: win_count += 1

            avg_return = np.mean(profits) if profits else 0
            std_return = np.std(profits) if profits else 1e-6
            sharpe = avg_return / std_return if std_return > 0 else 0
            win_rate = win_count / total_trades if total_trades > 0 else 0

            # 2. Drawdown-Aware Ranking
            max_drawdown = 0
            for idx in buy_indices:
                if idx + holding_period < len(self.df):
                    max_dd = min(self.df['Close'].iloc[idx:idx + holding_period]) / self.df['Close'].iloc[idx] - 1
                    max_drawdown = min(max_drawdown, max_dd)
            for idx in sell_indices:
                if idx + holding_period < len(self.df):
                    max_dd = 1 - max(self.df['Close'].iloc[idx:idx + holding_period]) / self.df['Close'].iloc[idx]
                    max_drawdown = min(max_drawdown, max_dd)

            # 3. Hit Ratio / Precision
            price_future = self.df['Close'].shift(-holding_period).values
            hit_buy = np.sum(price_future[buy_indices] > self.df['Close'].values[buy_indices])
            hit_sell = np.sum(price_future[sell_indices] < self.df['Close'].values[sell_indices])
            precision = (hit_buy + hit_sell) / (len(buy_indices) + len(sell_indices) + 1e-6)

            # 4. Signal Consistency
            consistency_streak = 0
            current_streak = 0
            for idx in range(len(signal_vals) - holding_period):
                if signal_vals[idx] == 2 and self.df['Close'].iloc[idx + holding_period] > self.df['Close'].iloc[idx]:
                    current_streak += 1
                elif signal_vals[idx] == 1 and self.df['Close'].iloc[idx + holding_period] < self.df['Close'].iloc[idx]:
                    current_streak += 1
                else:
                    consistency_streak = max(consistency_streak, current_streak)
                    current_streak = 0
            consistency_streak = max(consistency_streak, current_streak)

            # 5. Signal Strength vs Noise
            signal_density = total_trades / len(self.df)
            noise_penalty = abs(signal_density - 0.02)

            # 6. Volatility-Adjusted Effectiveness
            volatility = self.df['Close'].rolling(holding_period).std().mean()
            volatility_score = avg_return / (volatility + 1e-6)

            # Weighted Score
            score = (
                0.3 * avg_return +
                0.2 * sharpe +
                0.15 * win_rate +
                0.1 * (-max_drawdown) +
                0.1 * precision +
                0.05 * consistency_streak -
                0.05 * noise_penalty +
                0.05 * volatility_score
            )

            ranks.append([
                sig, avg_return, sharpe, win_rate, max_drawdown, precision,
                consistency_streak, signal_density, volatility_score, score,
                total_trades, len(buy_indices), len(sell_indices)
            ])

        ranks.sort(key=lambda x: x[-4], reverse=True)

        self.signal_accuracy_rank_df = pd.DataFrame(ranks, columns=[
            'Signal', 'AvgReturn', 'Sharpe', 'WinRate', 'MaxDrawdown', 'Precision',
            'ConsistencyStreak', 'SignalDensity', 'VolatilityScore', 'WeightedScore',
            'TotalTrades', 'BuyCount', 'SellCount'
        ])

        if self.verbose:
            print("\n===== Full Signal Ranking Summary =====")
            print(self.signal_accuracy_rank_df.head(10))

    
    def save_data(self):
      # Creating intermediate directories
      os.makedirs(self.dir_path, exist_ok=True)

      # Saving Feature Endineered Data
      if self.df is not None:
        self.feat_data_path = self.dir_path + 'feat_data.csv'
        self.df.to_csv(self.feat_data_path)
        if self.verbose:
          print('Feature Engineered Data Saved to ',self.feat_data_path)
      else:
        print('Feature Engineered Data Not Saved  since it was not created')

      # Saving Explored Signals Data
      if self.explored_signals_df is not None:
        self.explored_signals_path = self.dir_path + 'explored_signals.csv'
        self.explored_signals_df.to_csv(self.explored_signals_path)
        if self.verbose:
          print('Explored Signals Data Saved to ',self.explored_signals_path)
      else:
        print('Explored Signals Data Not Saved  since it was not created')

      if self.signal_accuracy_rank_df is not None:
        self.signal_accuracy_rank_path = self.dir_path + 'Ranked_Signals.csv'
        self.signal_accuracy_rank_df.to_csv(self.signal_accuracy_rank_path)
        if self.verbose:
          print('Ranked Signals Data Saved to ',self.signal_accuracy_rank_path)
      else:
        print('Ranked Signals Data Not Saved  since it was not created')

    


# In[7]:


'''
cols_to_keep = ['timestamp','Open', 'High', 'Low', 'Close', 'Volume', 'day_of_week', 'weekday','month', 'month_name', 'symbol']
df_path = 'X:/Autobot/crypto/BTC-USDT/sample_test/'
df_file_name = df_path+'feat_data.csv'
df = pd.read_csv(df_file_name, index_col=0, usecols=cols_to_keep)
df.index = pd.to_datetime(df.index)
#df.index.name=timestamps

print(len(df))
'''



# In[8]:


'''
indicators_list = ['VMA','MA','EMA','RSI','TR','ATR','VWAP']
trend_signals_list = ['SoloEMA','EMACrossover']
F = Feat_Eng(
    dir_path=df_path,
    df=df,
    indicators_list=indicators_list,
    trend_signals_list=trend_signals_list,
    verbose=True,  # Set False to disable prints
)

# Run full pipeline with selected steps
F.run(
    Indicators=True,
    Normalizations=False,        # Optional
    Trend_Signals=True,
    Reversal_Signals=True,
    Tot_Signals=True,
    Rank = True,
    Save=True                   # Prevents saving to file system during test
)
'''

