"""
TS_VLT_APP_0011 Protocol
"""
from parser_base import ProtocolParserBase
from datetime import datetime, timedelta
import json
import copy


class C_TS_VLT_APP_0011(ProtocolParserBase):

    RemainingData = str()
    PartialDataDetected = False

    def read(self):
        try:
            raw_data = self.socket.recv(4096).decode('utf-8')
        except UnicodeDecodeError:
            raw_data = self.socket.recv(4096).decode('latin-1')
        return raw_data.encode('unicode_escape').decode('ascii')
    def write(self, data):
        self.socket.sendall(data)

    def get_response_str(self, pkt_json_obj):
        response = str()
        if 'ak' in pkt_json_obj[0].keys():
            print("ak val = " + str(pkt_json_obj[0]['ak']))
            if int(pkt_json_obj[0]['ak']) == 1:
                response = "{\"status\":1}"
        #In all other cases there is no response
        print("Response = " + response)
        return response

    def convert_to_dict_list(self, pkt_json_obj):
        pkt_dict_list = list()
        svr_hit_time_stamp = datetime.utcnow()

        for i in pkt_json_obj:
            pkt_dict = copy.deepcopy(i)

            if 'ak' in pkt_json_obj[0]:
                pkt_dict["ak"] = pkt_json_obj[0]["ak"]

            pkt_dict["FV"] = pkt_json_obj[0]["FV"]
            pkt_dict["VD"] = pkt_json_obj[0]["VD"]
            pkt_dict["t"] = int(pkt_json_obj[0]["t"])
            pkt_dict["T"] = datetime.strptime(i["T"], "%Y-%m-%d %H:%M:%S")

            if 'SN' in i:
                pkt_dict["SN"] = int(i["SN"])

            if 'MV' in i:
                pkt_dict["MV"] = float(i["MV"])

            if 'o' in i:
                pkt_dict["o"] = float(i["o"])

            if 'O' in i:
                pkt_dict["O"] = float(i["O"])

            pkt_dict["s"] = float(i["s"])
            pkt_dict["CG"] = float(i["CG"])

            pkt_dict["FN"] = int(i["FN"])
            pkt_dict["SS"] = int(i["SS"])
            pkt_dict["svr_hit_ts"] = svr_hit_time_stamp + timedelta(hours=5,
                                                                    minutes=30)
            pkt_dict_list.append(pkt_dict)
        return pkt_dict_list

    def process(self, data):
        print("data rxd : " + data)

        response = str()

        data_scan_complete = False
        self.RemainingData += data

        while data_scan_complete is False:

            try:
                json_object = json.loads(self.RemainingData)
                print("Valid JSON Data : " + self.RemainingData)
                response = self.get_response_str(json_object)
                self.socket.sendall(bytes(response, 'utf-8'))
                """
                You may save raw data frame into database for debugging here
                """

                for i in json_object:
                    if i["v"] == "a":
                        print(i)
                        """
                        You may save valid data frames to database here
                        """

                pkt_dict_list = self.convert_to_dict_list(json_object)
                """ 
                You may log 'pkt_dict_list' list of packets in database here
                """

                self.RemainingData = str()
                data_scan_complete = True

            except ValueError as e:
                print("Is valid json? false")
                mix_up_idx = self.RemainingData.find('][')

                if mix_up_idx == -1:
                    #There is no multi packet mixup
                    if self.RemainingData[0] == '[':
                        pass
                    else:
                        print("Invalid start")
                        #Invalid data . So discard it
                        self.RemainingData = str()

                    data_scan_complete = True

                else:
                    #Multipacket mix detected

                    data_pkt_str = self.RemainingData[0:mix_up_idx + 1]

                    if data_pkt_str[0] == '[':
                        try:
                            json_object = json.loads(data_pkt_str)
                            print("Valid JSON Data : " + data_pkt_str)
                            response = self.get_response_str(json_object)
                            self.socket.sendall(bytes(response, 'utf-8'))
                            """
                            You may save raw data frame into database for debugging here
                            """

                            for i in json_object:
                                if i["v"] == "a":
                                    print(i)
                                    """
                                    You may save valid data frames to database here
                                    """

                            pkt_dict_list = self.convert_to_dict_list(
                                json_object)
                            """
                            You may log 'pkt_dict_list' list of packets in database here
                            """

                        except ValueError as e:
                            print("Json Error in : " + data_pkt_str)

                    else:
                        print("Invalid data " + data_pkt_str)
                        #discard the data

                    self.RemainingData = self.RemainingData[mix_up_idx + 1:]

        response = str()

        return response.encode('utf-8')

    def write_command(self, command):
        print("Command Call received : " + command)
        formatted_cmd = '{"command":"' + command + '"}'
        self.socket.sendall(bytes(formatted_cmd, 'utf-8'))

    def client_disconn_command(self, data):
        return data == '00'
