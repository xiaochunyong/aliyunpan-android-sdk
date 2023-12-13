package com.alicloud.databox.opensdk

import android.content.Context
import android.os.Build
import com.alicloud.databox.opensdk.auth.AliyunpanPKCECredentials
import com.alicloud.databox.opensdk.auth.AliyunpanServerCredentials
import com.alicloud.databox.opensdk.http.HttpHeaderInterceptor

class AliyunpanClientConfig private constructor(
    internal val context: Context,
    internal val scope: String,
    internal val baseApi: String,
    internal val credentials: AliyunpanCredentials
) : HttpHeaderInterceptor.HttpHeaderConfig {

    private val userAgent: String by lazy {
        val context = context
        val packageManager = context.packageManager
        val packageInfo = packageManager.getPackageInfo(context.packageName, 0)
        USER_AGENT_FORMAT.format(
            packageInfo.packageName.split(".").last(),
            packageInfo.versionName,
            packageInfo.packageName,
            getSdkBuild(),
            Build.VERSION.RELEASE,
            getSdkVersion()
        )
    }

    private fun getSdkVersion() = BuildConfig.VERSION

    private fun getSdkBuild() = "1"

    override fun getConfigUserAgent(): String {
        return userAgent
    }

    override fun getConfigAuthorization(): String? {
        return credentials.getAccessToken()
    }

    companion object {

        private const val USER_AGENT_FORMAT = "%s/%s (%s; build:{%s}; Android %s) AliyunpanSDK/%s"
    }

    class Builder {

        private val context: Context
        private val appId: String

        /**
         * Identifier
         * 默认用户标识
         */
        private var identifier: String = "sdk_user"

        /**
         * Scope
         * 默认的权限域
         * https://www.yuque.com/aliyundrive/zpfszx/dspik0
         */
        private var scope: String = "user:base,file:all:read"

        private val baseApi = "openapi.alipan.com"
//        private val baseApi = "stg-openapi.alipan.com"

        private var tokenServer: AliyunpanTokenServer? = null

        constructor(context: Context, appId: String) {
            this.context = context.applicationContext
            this.appId = appId
        }

        fun scope(scope: String) = apply { this.scope = scope }

        fun setIdentifier(identifier: String) = apply { this.identifier = identifier }

        fun tokenServer(tokenServer: AliyunpanTokenServer) = apply { this.tokenServer = tokenServer }

        fun build(): AliyunpanClientConfig {
            val aliyunpanTokenServer = tokenServer
            val credentials = if (aliyunpanTokenServer == null) {
                AliyunpanPKCECredentials(context, appId, identifier, baseApi)
            } else {
                AliyunpanServerCredentials(context, appId, identifier, baseApi, aliyunpanTokenServer)
            }

            return AliyunpanClientConfig(
                context,
                scope,
                baseApi,
                credentials
            )
        }
    }
}