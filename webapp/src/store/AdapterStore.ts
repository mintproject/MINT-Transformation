import { observable, action } from "mobx";
import axios from "axios";

export type AdapterType = {
  name: string,
  func_name: string,
  description: string,
  input: { [key: string]: any; },
  ouput: { [key: string]: any; }
};

// FIXME: should pass in backend url via env var
export const flaskUrl = "http://localhost:5000/api"

export class AdapterStore {
  @observable adapters: AdapterType[] = [];

  @action.bound getAdapters = () => {
    axios.get(`${flaskUrl}/adapters`).then(
      (resp) => {
        if ("data" in resp) {
          this.adapters = resp.data;
        } else {
          console.log("THERE IS SOMETHING WRONG!");
        }
      }
    );
  }
}