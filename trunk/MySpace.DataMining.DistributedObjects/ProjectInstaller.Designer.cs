namespace MySpace.DataMining.DistributedObjects5
{
    partial class ProjectInstaller
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.serviceProcessInstallerDO = new System.ServiceProcess.ServiceProcessInstaller();
            this.serviceInstallerDO = new System.ServiceProcess.ServiceInstaller();
            // 
            // serviceProcessInstallerDO
            // 
            this.serviceProcessInstallerDO.Password = null;
            this.serviceProcessInstallerDO.Username = null;
            // 
            // serviceInstallerDO
            // 
            this.serviceInstallerDO.ServiceName = "DistributedObjects";
            this.serviceInstallerDO.StartType = System.ServiceProcess.ServiceStartMode.Automatic;
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.serviceProcessInstallerDO,
            this.serviceInstallerDO});
            this.Committed += new System.Configuration.Install.InstallEventHandler(this.ProjectInstaller_Committed);

        }

        #endregion

        private System.ServiceProcess.ServiceProcessInstaller serviceProcessInstallerDO;
        private System.ServiceProcess.ServiceInstaller serviceInstallerDO;
    }
}