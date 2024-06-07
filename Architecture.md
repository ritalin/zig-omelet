## 起動

* ホストが、全ステージを起動する
* 各ステージが、ホストに、`launched`イベントを発行する
* ホストが、全stageに、`begin_topic`イベントを発行する
    * 各ステージが、ホストに、`topic`イベントを通知する
        * topic
    * 各ステージが、ホストに、`end_topic`イベントを発行する
* ホストが、ExtractStageに、`begin_session`イベントを発行する
    * 実際受け取るのは、WatchStageのみ
* FileWatchStageが、ホストに、`source` イベントを発行する
    * パスとハッシュ
    * ホストが、パスとハッシュから同期情報を構成する
* ホストが、ExtractStageに`source`イベントを発行する
    * パス
* ExtractStageが、ホストに、`topic_payload`イベントを発行する
    * topicとpayload
    * ホストが、同期情報を更新する
        * 8種が変更されていれば破棄する
* ホストが、全てのpayloadを受け取ったら、GenerateStageに、`begin_generate`イベントを発行する
    * ホストが、GenerateStageに、`topic_payload`イベントを発行する
        * topicとpayload
    * ホストが、GenerateStageに、`end_generate`イベントを発行する
* GenerateStageが、コード生成する

## Stage

* Extract
    * PlaceholderExtractStage
        * SQL
        * パラメータと型
    * SelectListExtractStage
        * 列名と型
        * 起動時にschemaを受け取っておく必要がある
    * FileWatchStage
        * ファイルの発行と変更監視
* Generate
    * SqlGenerateStage
        * placeholderを置換したSQLを保存する
    * TypescriptTygeGenerateStage
        * typescriptのコードを吐く

## 通信チャネル

* CMD
    * ホスト -> ステージ (Pub/Sub)
    * ステージ -> ホスト (Push/Pull)
* SRC
    * ExtractStage -> ホスト(Push/Pull)
    * ホスト -> ExtractStage (Pub/Sub)
* GEN
    * ホスト -> GenerateStage (Pub/Sub)

## 要検討

* ファイルの変更通知
    * WatchStageが、非同期で、ファイル変更の通知を受ける
        * WatchStageが、パスとハッシュを評価する
        * WatchStageが、ホストに、source イベントを発行する
* ホストで標準入力ハンドリング
    * `q`で終了
        * ホストが、全ステージに、`quit`イベントを発行する
            * ステージが、ホストに、`quit_accept`イベントを発行する
            * ステージを終了させる
* ログ
* oneshot