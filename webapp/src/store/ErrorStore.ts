import { observable, action } from "mobx";
// import axios from "axios";

export class ErrorStore {
  @observable isLoading: boolean = false;
  @observable hasFailed: boolean = false;
  @observable errorData: object = {};

  @action.bound setIsLoading = (isLoading: boolean) => {
    this.isLoading = isLoading;
  }

  @action.bound setHasFailed = (hasFailed: boolean) => {
    this.hasFailed = hasFailed;
  }

  @action.bound setErrorData = (errorData: object) => {
    this.errorData = errorData;
  }

}