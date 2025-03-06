package com.alicloud.databox.opensdk

import android.app.Activity
import okhttp3.OkHttpClient

interface AliyunpanBaseClient {

    fun clearOauth()

    fun fetchToken(activity: Activity)

    fun isInstallYunpanApp(): Boolean

    fun getOkHttpInstance(): OkHttpClient
}